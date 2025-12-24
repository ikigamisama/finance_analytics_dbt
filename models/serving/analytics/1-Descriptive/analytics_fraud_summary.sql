{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'insertion', 'fraud']
    )
}}

SELECT
    COUNT(DISTINCT alert_key) AS total_alerts,
    COUNT(DISTINCT CASE WHEN resolved_flag = 1 THEN alert_key END) AS resolved_alerts,
    COUNT(DISTINCT CASE WHEN confirmed_fraud_flag = 1 THEN alert_key END) AS confirmed_fraud,
    COUNT(DISTINCT CASE WHEN false_positive_flag = 1 THEN alert_key END) AS false_positives,
    
    -- Financial Impact
    ROUND(SUM(amount_recovered)::numeric, 2) AS total_recovered,
    ROUND(AVG(amount_recovered)::numeric, 2) AS avg_recovered,
    
    -- Performance
    ROUND(AVG(CASE WHEN resolved_flag = 1 THEN resolution_days END)::numeric, 1) AS avg_resolution_days,
    ROUND(COUNT(DISTINCT CASE WHEN confirmed_fraud_flag = 1 THEN alert_key END) * 100.0 / NULLIF(COUNT(DISTINCT CASE WHEN resolved_flag = 1 THEN alert_key END), 0), 2) AS fraud_confirmation_rate_pct,
    ROUND(COUNT(DISTINCT CASE WHEN false_positive_flag = 1 THEN alert_key END) * 100.0 / NULLIF(COUNT(DISTINCT alert_key), 0), 2) AS false_positive_rate_pct,
    
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_fraud_alerts') }}