{{
    config(
        materialized='table',
        schema='gold',
        tags=['analytics', 'descriptive', 'serving', 'products']
    )
}}

SELECT
    COUNT(DISTINCT product_key) AS total_products,
    COUNT(DISTINCT CASE WHEN is_premium THEN product_key END) AS premium_products,
    COUNT(DISTINCT CASE WHEN category = 'Deposit' THEN product_key END) AS deposit_products,
    COUNT(DISTINCT CASE WHEN category = 'Credit' THEN product_key END) AS credit_products,
    COUNT(DISTINCT CASE WHEN category = 'Loan' THEN product_key END) AS loan_products,
    COUNT(DISTINCT CASE WHEN category = 'Investment' THEN product_key END) AS investment_products,
    
    -- Pricing
    ROUND(AVG(CASE WHEN interest_rate > 0 THEN interest_rate_pct END), 2) AS avg_interest_rate_pct,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('dim_product') }}