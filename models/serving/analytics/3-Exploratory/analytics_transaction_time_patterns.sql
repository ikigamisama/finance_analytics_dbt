{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'transactions']
    )
}}

SELECT
    d.day_name,
    d.day_of_week,
    t.transaction_hour,
    t.channel,
    t.merchant_category,
    
    COUNT(*) AS transaction_count,
    ROUND(AVG(t.transaction_amount_abs)::numeric, 2) AS avg_amount,
    COUNT(DISTINCT t.customer_key) AS unique_customers,
    
    -- Fraud patterns
    SUM(t.is_fraud_flag::int) AS fraud_count,
    ROUND(SUM(t.is_fraud_flag::int) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    
    -- High value patterns
    SUM(t.is_high_value_flag::int) AS high_value_count,
    ROUND(SUM(t.is_high_value_flag::int) * 100.0 / COUNT(*), 2) AS high_value_pct,
    
    -- International patterns
    SUM(CASE WHEN t.is_international THEN 1 ELSE 0 END) AS international_count,
    ROUND(SUM(CASE WHEN t.is_international THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS international_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY d.day_name, d.day_of_week, t.transaction_hour, t.channel, t.merchant_category
ORDER BY d.day_of_week, t.transaction_hour