{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'dimension', 'serving', 'merchant']
    )
}}

WITH merchant_enhanced AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} AS merchant_key,
        merchant_id AS merchant_natural_key,
        merchant_name,
        category,
        mcc_code,
        category_group,
        city,
        state,
        country,
        latitude,
        longitude,
        region,
        risk_rating,
        risk_score,
        avg_transaction_amount,
        transaction_value_segment,
        is_online,
        merchant_type,
        established_date,
        years_in_business,
        business_maturity,
        mcc_category,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM {{ ref('stg_merchants') }}
)

SELECT * FROM merchant_enhanced