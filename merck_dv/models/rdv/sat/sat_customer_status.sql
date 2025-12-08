{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{%- set source_model = ref('stg_customer') -%}

WITH staged AS (

    -- 1. STAGE: Select and rename columns from the staging layer to match DV standards
    SELECT
        -- Map the staging keys/hashes to the satellite's expected column names, using _HK for consistency
        customer_hk AS CUSTOMER_HK,
        customer_hashdiff AS CUSTOMER_STATUS_HASHDIFF,

        -- Map descriptive columns
        c_mktsegment AS CUSTOMER_STATUS_CODE,
        load_ts AS STATUS_EFFECTIVE_DATE,

        -- Metadata
        rsrc AS SOURCE,
        CURRENT_TIMESTAMP()::timestamp_ntz AS LOAD_DATE
    FROM {{ source_model }}
)

{% if is_incremental() %}

, latest_sat AS (

    -- 2. LATEST_SAT: Find the most recent record for each parent key in the existing satellite
    SELECT
        CUSTOMER_HK,
        CUSTOMER_STATUS_HASHDIFF,
        LOAD_DATE
    FROM (
        SELECT
            *,
            -- Partition by CUSTOMER_HK
            ROW_NUMBER() OVER(PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATE DESC) AS rn
        FROM {{ this }}
    ) AS a
    WHERE a.rn = 1

),

records_to_insert AS (

    -- 3. RECORDS_TO_INSERT: Select staged records that are new or represent a change
    SELECT
        stg.CUSTOMER_HK,
        stg.CUSTOMER_STATUS_HASHDIFF,
        stg.CUSTOMER_STATUS_CODE,
        stg.STATUS_EFFECTIVE_DATE,
        stg.LOAD_DATE,
        stg.SOURCE
    FROM staged AS stg
    LEFT JOIN latest_sat
        ON stg.CUSTOMER_HK = latest_sat.CUSTOMER_HK
    WHERE
        -- Condition 1: Key is completely new
        latest_sat.CUSTOMER_HK IS NULL
        -- Condition 2: Key exists, but the hashdiff has changed
        OR stg.CUSTOMER_STATUS_HASHDIFF != latest_sat.CUSTOMER_STATUS_HASHDIFF
)

-- Final SELECT for Incremental Run
SELECT * FROM records_to_insert

{% else %}

-- Final SELECT for Full Refresh
SELECT
    CUSTOMER_HK,
    CUSTOMER_STATUS_HASHDIFF,
    CUSTOMER_STATUS_CODE,
    STATUS_EFFECTIVE_DATE,
    LOAD_DATE,
    SOURCE
FROM staged

{% endif %}