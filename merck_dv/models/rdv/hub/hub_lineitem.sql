{{ config(
    materialized='table', 
    unique_key='lineitem_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_lineitem') }}
),
deduped_source AS (
    SELECT 
        lineitem_hk,
        l_orderkey,
        l_linenumber,
        load_ts,
        rsrc,
        ROW_NUMBER() OVER (PARTITION BY lineitem_hk ORDER BY load_ts, rsrc) as rn
    FROM source 
    WHERE lineitem_hk IS NOT NULL 
      AND l_orderkey IS NOT NULL
      AND l_linenumber IS NOT NULL
),
new_lineitems AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
)
SELECT 
    lineitem_hk,
    l_orderkey || '-' || l_linenumber as lineitem_nk, -- Composite Natural Key (e.g., '1-1', '1-2')
    load_ts,
    rsrc,
    CURRENT_TIMESTAMP() as first_seen_ts
FROM new_lineitems