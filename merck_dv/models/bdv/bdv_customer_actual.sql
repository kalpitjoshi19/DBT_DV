{{ config(
    materialized='view',
    schema='BDV'
) }}

-- Define Hub, Attribute Satellite, and Record Tracking Satellite (RTS) references
{%- set hub_model = ref('hub_customer') -%}
{%- set sat_attribute_model = ref('sat_customer') -%}
{%- set sat_tracking_model = ref('sat_customer_tracking') -%}

-- Start the single Common Table Expression (CTE) block. All steps are separated by commas.
WITH latest_customer_details AS (
    -- --- 1a. LOGIC FOR CUSTOMER ATTRIBUTES: Find the single latest record (rn=1) from the SAT_CUSTOMER ---
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMER_HK
            ORDER BY LOAD_TS DESC
        ) AS rn
    FROM {{ sat_attribute_model }}
),

actual_attributes AS (
    -- 1b. Filter to only the most current descriptive attributes
    SELECT
        CUSTOMER_HK,
        LOAD_TS AS ATTRIBUTE_LOAD_TS,
        C_NAME,
        C_ADDRESS,
        C_PHONE,
        C_ACCTBAL,
        C_MKTSEGMENT,
        C_COMMENT,
        C_NATIONKEY
    FROM latest_customer_details
    WHERE rn = 1
),

-- --- 2a. LOGIC FOR DELETION STATUS: Find the system's latest load date (needed for status determination) ---
max_load_date AS (
    SELECT
        MAX(load_date) AS latest_load_date
    FROM {{ sat_tracking_model }}
),

-- --- 2b. Identify the latest tracking record (rn = 1) for every customer key in the RTS ---
rts_current_status AS (
    SELECT
        CUSTOMER_HK,
        LOAD_DATE,
        -- This window function is safe because it's inside the CTE and correctly uses ORDER BY
        LAG(LOAD_DATE, 1) OVER (PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATE) AS PREVIOUS_LOAD_DATE,
        ROW_NUMBER() OVER (PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATE DESC) AS rn
    FROM {{ sat_tracking_model }}
),

-- --- 2c. Determine the actual status by comparing the RTS to the max system load date ---
actual_status AS (
    SELECT
        rts.CUSTOMER_HK,
        rts.LOAD_DATE AS RTS_LOAD_DATE,
        rts.PREVIOUS_LOAD_DATE,
        
        CASE
            -- If the key was last seen during the most recent system load, it is PRESENT.
            WHEN rts.LOAD_DATE = mld.latest_load_date THEN 'PRESENT' 
            -- If the key was last seen BEFORE the most recent system load, it is SOFT DELETED.
            WHEN rts.LOAD_DATE < mld.latest_load_date THEN 'SOFT DELETED'
            ELSE 'ERROR' -- Should ideally not happen if data is consistent
        END AS CUSTOMER_STATUS
        
    FROM rts_current_status rts
    CROSS JOIN max_load_date mld
    WHERE rts.rn = 1 -- Only the most recent tracking record
),

-- --- 3. Hub Customer (For Natural Key and joining) ---
hub_customer AS (
    SELECT
        CUSTOMER_HK,
        CUSTOMER_NK
    FROM {{ hub_model }}
)

-- --- 4. FINAL COMBINATION ---
SELECT
    h.CUSTOMER_HK,
    h.CUSTOMER_NK,
    
    -- Status & Tracking Info
    s.CUSTOMER_STATUS,
    s.RTS_LOAD_DATE AS LAST_SEEN_DATE,
    s.PREVIOUS_LOAD_DATE,
    
    -- Attribute Info
    a.ATTRIBUTE_LOAD_TS,
    a.C_NAME,
    a.C_ADDRESS,
    a.C_PHONE,
    a.C_ACCTBAL,
    a.C_MKTSEGMENT,
    a.C_COMMENT,
    a.C_NATIONKEY
FROM hub_customer h
INNER JOIN actual_attributes a
    ON h.CUSTOMER_HK = a.CUSTOMER_HK
LEFT JOIN actual_status s
    ON h.CUSTOMER_HK = s.CUSTOMER_HK