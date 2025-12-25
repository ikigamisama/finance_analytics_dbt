{{
    config(
        materialized='view',
        schema="gold",
        tags=['analytics', 'realtime', 'serving', 'fraud']
    )
}}

SELECT
    t.transaction_id,
    t.transaction_date,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - t.transaction_date)) / 60 AS minutes_ago,
    
    t.customer_key,
    c.customer_segment,
    c.churn_risk_category,
    
    t.transaction_amount_abs,
    t.merchant_category,
    t.channel,
    t.is_international,
    t.location_city,
    t.location_country,
    
    -- Fraud indicators
    ROUND((t.fraud_score * 100)::numeric, 2) AS fraud_score_pct,
    t.fraud_risk_category,
    t.is_fraud_flag AS confirmed_fraud,
    
    -- Risk factors
    ROUND(t.distance_from_home_km::numeric, 1) AS distance_from_home_km,
    ROUND(t.merchant_risk_score::numeric, 2) AS merchant_risk_score,
    t.velocity_24h,
    ROUND(t.amount_deviation_score::numeric, 2) AS amount_deviation_score,
    
    -- Alert priority
    CASE
        WHEN t.fraud_score >= 0.9 AND t.transaction_amount_abs > 5000 THEN 'CRITICAL'
        WHEN t.fraud_score >= 0.8 THEN 'HIGH'
        WHEN t.fraud_score >= 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS alert_priority,
    
    -- Recommended action
    CASE
        WHEN t.fraud_score >= 0.9 THEN 'Block & Contact Customer Immediately'
        WHEN t.fraud_score >= 0.7 THEN 'Hold for Review'
        WHEN t.fraud_score >= 0.5 THEN 'Monitor'
        ELSE 'Auto-Approve'
    END AS recommended_action,
    
    -- Investigation status (from fraud alerts table if exists)
    COALESCE(fa.investigation_status, 'New Alert') AS investigation_status,
    fa.assigned_to,
    
    CURRENT_TIMESTAMP AS alert_generated_at
    
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_customer') }} c ON t.customer_key = c.customer_key
LEFT JOIN {{ ref('fact_fraud_alerts') }} fa ON t.transaction_key = fa.transaction_key
    
WHERE t.transaction_date >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
  AND (t.fraud_score >= 0.5 OR t.is_fraud_flag = TRUE)
  AND c.is_current = TRUE
  
ORDER BY t.fraud_score DESC, t.transaction_date DESC
LIMIT 100