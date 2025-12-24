{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'channel']
    )
}}

WITH customer_channel_usage AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        c.preferred_channel,
        
        -- Channel usage distribution
        COUNT(DISTINCT t.transaction_key) AS total_transactions,
        
        COUNT(DISTINCT CASE WHEN t.channel = 'Mobile' THEN t.transaction_key END) AS mobile_count,
        COUNT(DISTINCT CASE WHEN t.channel = 'Online' THEN t.transaction_key END) AS online_count,
        COUNT(DISTINCT CASE WHEN t.channel = 'Branch' THEN t.transaction_key END) AS branch_count,
        COUNT(DISTINCT CASE WHEN t.channel = 'ATM' THEN t.transaction_key END) AS atm_count,
        
        -- Dominant channel
        MODE() WITHIN GROUP (ORDER BY t.channel) AS dominant_channel,
        COUNT(DISTINCT t.channel) AS channels_used,
        
        -- Outcome metrics
        c.customer_lifetime_value,
        c.churn_risk_score,
        COALESCE(i.avg_satisfaction, 0) AS avg_satisfaction,
        COALESCE(i.interaction_count, 0) AS service_interactions
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON c.customer_key = t.customer_key
        AND t.transaction_date >= CURRENT_DATE - INTERVAL '180 days'
    LEFT JOIN (
        SELECT
            customer_key,
            AVG(satisfaction_rating) AS avg_satisfaction,
            COUNT(*) AS interaction_count
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, c.customer_segment, c.preferred_channel,
             c.customer_lifetime_value, c.churn_risk_score,
             i.avg_satisfaction, i.interaction_count
)

SELECT
    dominant_channel,
    customer_segment,
    
    -- Channel adoption metrics
    CASE
        WHEN channels_used = 1 THEN 'Single-Channel'
        WHEN channels_used = 2 THEN 'Dual-Channel'
        ELSE 'Multi-Channel'
    END AS channel_strategy,
    
    COUNT(*) AS customer_count,
    
    -- Digital adoption percentage
    ROUND(
        AVG(
            (mobile_count + online_count) * 100.0 / 
            NULLIF(total_transactions, 0)
        )
    , 2) AS digital_adoption_pct,
    
    -- Outcome metrics
    ROUND(AVG(avg_satisfaction)::numeric, 2) AS avg_satisfaction_rating,
    ROUND(AVG(customer_lifetime_value)::numeric, 2) AS avg_clv,
    ROUND(AVG(churn_risk_score)::numeric * 100, 2) AS avg_churn_risk_pct,
    ROUND(AVG(service_interactions)::numeric, 1) AS avg_service_interactions,
    
    -- Causal effects (compared to Branch baseline)
    ROUND(
        AVG(avg_satisfaction)::numeric - 
        MAX(CASE WHEN dominant_channel = 'Branch' THEN AVG(avg_satisfaction) OVER (PARTITION BY customer_segment) END)
    , 2) AS satisfaction_effect_vs_branch,
    
    ROUND(
        AVG(churn_risk_score)::numeric - 
        MAX(CASE WHEN dominant_channel = 'Branch' THEN AVG(churn_risk_score) OVER (PARTITION BY customer_segment) END)
    , 4) AS churn_effect_vs_branch,
    
    -- Multi-channel premium effect
    ROUND(
        AVG(CASE WHEN channels_used >= 2 THEN customer_lifetime_value END)::numeric -
        AVG(CASE WHEN channels_used = 1 THEN customer_lifetime_value END)::numeric
    , 2) AS multichannel_clv_premium,
    
    -- Causal interpretation
    CASE
        WHEN dominant_channel = 'Branch' THEN 'Traditional Channel (Baseline)'
        WHEN dominant_channel IN ('Mobile', 'Online') THEN 'Digital Channel (Modernized)'
        ELSE 'Hybrid Channel'
    END AS channel_type,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM customer_channel_usage
GROUP BY dominant_channel, customer_segment, channel_strategy
ORDER BY customer_segment, channel_strategy, dominant_channel