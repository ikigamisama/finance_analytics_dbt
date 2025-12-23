{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'seasonality']
    )
}}

SELECT
    d.month,
    d.month_name,
    d.quarter,
    d.day_of_week,
    d.day_name,
    t.merchant_category,
    
    COUNT(*) AS transaction_count,
    ROUND(SUM(t.transaction_amount_abs), 2) AS total_volume,
    ROUND(AVG(t.transaction_amount_abs), 2) AS avg_amount,
    COUNT(DISTINCT t.customer_key) AS unique_customers,
    
    -- Year-over-year comparison helper
    d.year,
    
    -- Seasonal indicators
    CASE
        WHEN d.month IN (12, 1, 2) THEN 'Winter'
        WHEN d.month IN (3, 4, 5) THEN 'Spring'
        WHEN d.month IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '730 days'  -- 2 years
GROUP BY d.month, d.month_name, d.quarter, d.day_of_week, d.day_name, 
         t.merchant_category, d.year
ORDER BY d.year DESC, d.month, d.day_of_week