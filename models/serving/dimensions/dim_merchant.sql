{{
    config(
        materialized='table',
        tags=['gold', 'serving', 'dimension']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_merchants') }}
),

merchant_dimension AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} AS merchant_key,
        
        -- Natural Key
        merchant_id,
        
        -- Merchant Details
        merchant_name,
        category,
        category_group,
        mcc_code,
        mcc_category,
        
        -- Location
        city,
        state,
        country,
        region,
        latitude,
        longitude,
        
        -- Risk Profile
        risk_rating,
        risk_score,
        avg_transaction_amount,
        transaction_value_segment,
        
        -- Merchant Type
        is_online,
        merchant_type,
        
        -- Business Metrics
        established_date,
        years_in_business,
        business_maturity,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dw_created_at,
        CURRENT_TIMESTAMP AS dw_updated_at
        
    FROM source
)

SELECT * FROM merchant_dimension;