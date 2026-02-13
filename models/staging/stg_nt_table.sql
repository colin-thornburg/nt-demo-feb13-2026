/*
    Staging Model: stg_nt_table
    
    Purpose: Prepare raw source data for Data Vault loading.
    - Adds audit columns (AUDIT_ID, LOAD_DTS, RECORD_SOURCE)
    - Generates hash keys (HK_FIRM) for Hub loading
    - Generates hashdiff (HD_FIRM_DETAILS) for Satellite change detection
    
    This is the equivalent of what AutomateDV's stage() macro produces,
    written as plain SQL for full transparency and zero package dependencies.
*/

WITH source AS (

    SELECT * FROM {{ ref('NT_table') }}

),

staged AS (

    SELECT
        -- ============================================
        -- Hash Key: Business key for Hub
        -- ============================================
        MD5(
            COALESCE(CAST(FIRM_ID AS VARCHAR), '^^')
        ) AS HK_FIRM,

        -- ============================================
        -- Hashdiff: Change detection for Satellite
        -- Alpha-sorted columns per Data Vault best practice
        -- ============================================
        MD5(
            CONCAT_WS('||',
                COALESCE(CAST(AMOUNT AS VARCHAR), '^^'),
                COALESCE(CAST(BASE_CURRENCY AS VARCHAR), '^^'),
                COALESCE(CAST(DESCRIPTION AS VARCHAR), '^^'),
                COALESCE(CAST(EMAIL AS VARCHAR), '^^'),
                COALESCE(CAST(LONG_SHORT_IND AS VARCHAR), '^^'),
                COALESCE(CAST(NAME AS VARCHAR), '^^')
            )
        ) AS HD_FIRM_DETAILS,

        -- ============================================
        -- Business Keys
        -- ============================================
        FIRM_ID,

        -- ============================================
        -- Payload (descriptive attributes)
        -- ============================================
        NAME,
        EMAIL,
        DESCRIPTION,
        AMOUNT,
        LONG_SHORT_IND,
        BASE_CURRENCY,

        -- ============================================
        -- Audit Columns (Customer Requirement #2)
        -- ============================================
        CURRENT_TIMESTAMP() AS LOAD_DTS,
        MD5(
            CONCAT(
                CAST(FIRM_ID AS VARCHAR), '_',
                CAST(CURRENT_TIMESTAMP() AS VARCHAR)
            )
        ) AS AUDIT_ID,
        'NT_TABLE' AS RECORD_SOURCE

    FROM source

)

SELECT * FROM staged
