/*
    Hub: hub_firm
    
    Purpose: Store unique business keys for the FIRM entity.
    - Insert-only: once a business key is recorded, it never changes.
    - Tracks when each key was first seen (LOAD_DTS) and its origin (RECORD_SOURCE).
    
    Data Vault Pattern: Hub = unique business keys + metadata
*/

{{
    config(
        materialized='incremental',
        unique_key='HK_FIRM'
    )
}}

WITH source AS (

    SELECT
        HK_FIRM,
        FIRM_ID,
        LOAD_DTS,
        RECORD_SOURCE
    FROM {{ ref('stg_nt_table') }}

),

{% if is_incremental() %}

-- Only load business keys we haven't seen before
new_records AS (

    SELECT
        src.HK_FIRM,
        src.FIRM_ID,
        src.LOAD_DTS,
        src.RECORD_SOURCE
    FROM source src
    LEFT JOIN {{ this }} tgt
        ON src.HK_FIRM = tgt.HK_FIRM
    WHERE tgt.HK_FIRM IS NULL

)

SELECT * FROM new_records

{% else %}

-- Initial load: deduplicate on business key, keep earliest record
initial_load AS (

    SELECT
        HK_FIRM,
        FIRM_ID,
        LOAD_DTS,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY HK_FIRM
            ORDER BY LOAD_DTS ASC
        ) AS row_num
    FROM source

)

SELECT
    HK_FIRM,
    FIRM_ID,
    LOAD_DTS,
    RECORD_SOURCE
FROM initial_load
WHERE row_num = 1

{% endif %}
