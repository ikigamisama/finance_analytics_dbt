{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'transactions']
    )
}}

SELECT
    t.decline_reason,
    t.channel,
    t.merchant_category,
    t.is_international,
    
    COUNT(*) AS declined_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_declines,
    ROUND(AVG(t.transaction_amount_abs)::numeric, 2) AS avg_declined_amount,
    COUNT(DISTINCT t.customer_key) AS affected_customers,
    COUNT(DISTINCT t.merchant_key) AS affected_merchants,
    
    -- Time patterns
    ROUND(AVG(EXTRACT(HOUR FROM t.transaction_date)), 1) AS avg_hour_of_day,
    
    -- Risk indicators
    ROUND(AVG(t.fraud_score)::numeric, 3) AS avg_fraud_score,
    ROUND(AVG(t.merchant_risk_score)::numeric, 2) AS avg_merchant_risk,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_transactions') }} t
WHERE t.is_declined_flag = 1
  AND t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY t.decline_reason, t.channel, t.merchant_category, t.is_international
ORDER BY declined_count DESC