{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'fraud']
    )
}}

SELECT
    t.merchant_category,
    t.channel,
    t.is_international,
    d.day_name,
    d.is_weekend,
    EXTRACT(HOUR FROM t.transaction_date) AS hour_of_day,
    
    -- Fraud Metrics
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN t.is_fraud_flag = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(SUM(CASE WHEN t.is_fraud_flag = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(SUM(t.fraud_amount), 2) AS total_fraud_amount,
    ROUND(AVG(CASE WHEN t.is_fraud_flag = 1 THEN t.fraud_score END), 3) AS avg_fraud_score,
    
    -- Transaction Patterns
    ROUND(AVG(t.transaction_amount_abs), 2) AS avg_transaction_amount,
    ROUND(AVG(t.distance_from_home_km), 1) AS avg_distance_from_home,
    ROUND(AVG(t.velocity_24h), 1) AS avg_velocity_24h,
    
    -- Customer Risk
    COUNT(DISTINCT t.customer_key) AS unique_customers,
    COUNT(DISTINCT CASE WHEN t.is_fraud_flag = 1 THEN t.customer_key END) AS fraud_customers,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY t.merchant_category, t.channel, t.is_international, d.day_name, 
         d.is_weekend, EXTRACT(HOUR FROM t.transaction_date)
HAVING COUNT(*) >= 100  -- Filter for statistical significance
ORDER BY fraud_rate_pct DESC
LIMIT 100