/*
    Satellite: sat_firm_details
    
    Purpose: Track changes in descriptive attributes for each FIRM.
    - Uses hashdiff (HD_FIRM_DETAILS) to detect changes in payload columns.
    - Only inserts a new record when attribute values actually change.
    - Preserves full history: every version of every firm's details is retained.
    
    Data Vault Pattern: Satellite = hash key + hashdiff + payload + metadata
*/

{{
    config(
        materialized='incremental',
        unique_key=['HK_FIRM', 'LOAD_DTS']
    )
}}

WITH source AS (

    SELECT
        HK_FIRM,
        HD_FIRM_DETAILS,
        NAME,
        EMAIL,
        DESCRIPTION,
        AMOUNT,
        LONG_SHORT_IND,
        BASE_CURRENCY,
        LOAD_DTS,
        RECORD_SOURCE
    FROM {{ ref('stg_nt_table') }}

),

{% if is_incremental() %}

-- Only load records where the hashdiff has changed
-- (i.e., one or more payload columns have a new value)
latest_satellite AS (

    SELECT
        HK_FIRM,
        HD_FIRM_DETAILS
    FROM (
        SELECT
            HK_FIRM,
            HD_FIRM_DETAILS,
            ROW_NUMBER() OVER (
                PARTITION BY HK_FIRM
                ORDER BY LOAD_DTS DESC
            ) AS row_num
        FROM {{ this }}
    )
    WHERE row_num = 1

),

new_records AS (

    SELECT
        src.HK_FIRM,
        src.HD_FIRM_DETAILS,
        src.NAME,
        src.EMAIL,
        src.DESCRIPTION,
        src.AMOUNT,
        src.LONG_SHORT_IND,
        src.BASE_CURRENCY,
        src.LOAD_DTS,
        src.RECORD_SOURCE
    FROM source src
    LEFT JOIN latest_satellite sat
        ON src.HK_FIRM = sat.HK_FIRM
    WHERE sat.HK_FIRM IS NULL
       OR src.HD_FIRM_DETAILS != sat.HD_FIRM_DETAILS

)

SELECT * FROM new_records

{% else %}

-- Initial load: deduplicate, keep earliest version per hash key
initial_load AS (

    SELECT
        HK_FIRM,
        HD_FIRM_DETAILS,
        NAME,
        EMAIL,
        DESCRIPTION,
        AMOUNT,
        LONG_SHORT_IND,
        BASE_CURRENCY,
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
    HD_FIRM_DETAILS,
    NAME,
    EMAIL,
    DESCRIPTION,
    AMOUNT,
    LONG_SHORT_IND,
    BASE_CURRENCY,
    LOAD_DTS,
    RECORD_SOURCE
FROM initial_load
WHERE row_num = 1

{% endif %}
