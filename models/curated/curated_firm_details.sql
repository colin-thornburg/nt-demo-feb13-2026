/*
    Curated Layer: curated_firm_details
    
    Purpose: Apply business rules on top of the Raw Vault.
    - Joins Hub and Satellite to produce a current-state view.
    - Transforms email domain from '@example.com' to '@ntrs.com' (Customer Requirement #3).
    - Business logic is cleanly separated from raw data ingestion.
    
    Data Vault Principle: Business rules belong in the Business Vault / Curated layer,
    never in the Raw Vault. If rules change, raw data is untouched and reprocessable.
*/

WITH current_satellite AS (

    -- Get the most recent version of each firm's details
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
            ORDER BY LOAD_DTS DESC
        ) AS row_num
    FROM {{ ref('sat_firm_details') }}

),

hub AS (

    SELECT
        HK_FIRM,
        FIRM_ID
    FROM {{ ref('hub_firm') }}

),

joined AS (

    SELECT
        h.FIRM_ID,
        h.HK_FIRM,
        s.NAME,
        
        -- ============================================
        -- Business Rule: Email Domain Transformation
        -- Customer Requirement #3
        -- ============================================
        REPLACE(s.EMAIL, '@example.com', '@ntrs.com') AS EMAIL,
        
        s.DESCRIPTION,
        s.AMOUNT,
        s.LONG_SHORT_IND,
        s.BASE_CURRENCY,
        s.LOAD_DTS,
        s.RECORD_SOURCE
    FROM hub h
    INNER JOIN current_satellite s
        ON h.HK_FIRM = s.HK_FIRM
    WHERE s.row_num = 1

)

SELECT * FROM joined
