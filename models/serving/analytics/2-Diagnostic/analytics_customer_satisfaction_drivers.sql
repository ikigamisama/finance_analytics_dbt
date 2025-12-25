{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'service']
    )
}}

SELECT
    i.interaction_type,
    i.reason,
    i.duration_category,
    i.issue_severity,
    
    COUNT(*) AS interaction_count,
    ROUND(AVG(i.satisfaction_rating)::numeric, 2) AS avg_satisfaction,
    ROUND(AVG(i.duration_minutes)::numeric, 1) AS avg_duration_minutes,
    ROUND(AVG(i.sentiment_score)::numeric, 3) AS avg_sentiment_score,
    
    -- Resolution metrics
    SUM(i.resolved_flag) AS resolved_count,
    ROUND((SUM(i.resolved_flag) * 100.0 / COUNT(*))::numeric, 2) AS resolution_rate_pct,
    SUM(i.escalated_flag) AS escalated_count,
    ROUND((SUM(i.escalated_flag) * 100.0 / COUNT(*))::numeric, 2) AS escalation_rate_pct,
    
    -- Sentiment breakdown
    SUM(i.positive_sentiment_flag) AS positive_count,
    SUM(i.negative_sentiment_flag) AS negative_count,
    ROUND((SUM(i.negative_sentiment_flag) * 100.0)::numeric / COUNT(*), 2) AS negative_sentiment_pct,
    
    -- Customer impact
    COUNT(DISTINCT i.customer_key) AS unique_customers,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_customer_interactions') }} i
WHERE i.interaction_date >= CURRENT_DATE - INTERVAL '180 days'
GROUP BY i.interaction_type, i.reason, i.duration_category, i.issue_severity
ORDER BY interaction_count DESC