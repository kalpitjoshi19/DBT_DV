{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{%- set source_model = ref('stg_customer') -%}

-- This Record Tracking Satellite (RTS) tracks the existence of a customer 
-- key in every load batch. It inserts a record for every key on every run 
-- to build a timeline of presence, which is used by the Business Vault 
-- to detect soft deletions.

WITH staged AS (
    -- 1. STAGE: Select the Hub Key and set the load metadata.
    -- The EXISTENCE_HASHDIFF is a constant value, as the RTS only tracks existence.
    SELECT
        customer_hk AS CUSTOMER_HK,
        -- Use a constant value for the hashdiff since the payload (existence) never changes.
        MD5('RTS_CUSTOMER_EXISTENCE') AS EXISTENCE_HASHDIFF, 
        
        load_ts AS LOAD_TS,
        rsrc AS SOURCE,
        -- This is the unique batch timestamp, crucial for deletion detection.
        CURRENT_TIMESTAMP()::timestamp_ntz AS LOAD_DATE 
    FROM {{ source_model }}
)

-- Final SELECT: Inserts all unique keys from the current staging table.
-- This logic is the same for both full refresh and incremental loads.
SELECT DISTINCT
    CUSTOMER_HK,
    EXISTENCE_HASHDIFF,
    LOAD_TS,
    LOAD_DATE,
    SOURCE
FROM staged