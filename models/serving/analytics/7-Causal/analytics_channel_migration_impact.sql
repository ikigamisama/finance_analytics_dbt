{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'channel']
    )
}}

WITH customer_channel_usage AS (
    -- (UNCHANGED: your original CTE)
    SELECT
        c.customer_key,
        c.customer_segment,
        c.preferred_channel,
        
        COUNT(DISTINCT t.transaction_key) AS total_transactions,
        COUNT(DISTINCT CASE WHEN t.channel = 'Mobile' THEN t.transaction_key END) AS mobile_count,
        COUNT(DISTINCT CASE WHEN t.channel = 'Online' THEN t.transaction_key END) AS online_count,
        COUNT(DISTINCT CASE WHEN t.channel = 'Branch' THEN t.transaction_key END) AS branch_count,
        COUNT(DISTINCT CASE WHEN t.channel = 'ATM' THEN t.transaction_key END) AS atm_count,
        
        MODE() WITHIN GROUP (ORDER BY t.channel) AS dominant_channel,
        COUNT(DISTINCT t.channel) AS channels_used,
        
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
    GROUP BY
        c.customer_key,
        c.customer_segment,
        c.preferred_channel,
        c.customer_lifetime_value,
        c.churn_risk_score,
        i.avg_satisfaction,
        i.interaction_count
),

channel_aggregates AS (
    SELECT
        dominant_channel,
        customer_segment,
        
        CASE
            WHEN channels_used = 1 THEN 'Single-Channel'
            WHEN channels_used = 2 THEN 'Dual-Channel'
            ELSE 'Multi-Channel'
        END AS channel_strategy,
        
        COUNT(*) AS customer_count,
        
        AVG((mobile_count + online_count) * 100.0 / NULLIF(total_transactions, 0))
            AS digital_adoption_pct,
        
        AVG(avg_satisfaction) AS avg_satisfaction_rating,
        AVG(customer_lifetime_value) AS avg_clv,
        AVG(churn_risk_score) AS avg_churn_risk,
        AVG(service_interactions) AS avg_service_interactions
        
    FROM customer_channel_usage
    GROUP BY dominant_channel, customer_segment, channel_strategy
),

branch_baseline AS (
    SELECT
        customer_segment,
        avg_satisfaction_rating AS branch_satisfaction,
        avg_churn_risk AS branch_churn_risk
    FROM channel_aggregates
    WHERE dominant_channel = 'Branch'
)

SELECT
    ca.dominant_channel,
    ca.customer_segment,
    ca.channel_strategy,
    ca.customer_count,
    
    ROUND(ca.digital_adoption_pct, 2) AS digital_adoption_pct,
    ROUND(ca.avg_satisfaction_rating::numeric, 2) AS avg_satisfaction_rating,
    ROUND(ca.avg_clv::numeric, 2) AS avg_clv,
    ROUND((ca.avg_churn_risk * 100)::numeric, 2) AS avg_churn_risk_pct,
    ROUND(ca.avg_service_interactions::numeric, 1) AS avg_service_interactions,
    
    -- Effects vs Branch baseline
    ROUND((ca.avg_satisfaction_rating - bb.branch_satisfaction)::numeric, 2)
        AS satisfaction_effect_vs_branch,
    
    ROUND((ca.avg_churn_risk - bb.branch_churn_risk)::numeric, 4)
        AS churn_effect_vs_branch,
    
    -- Multi-channel premium
    ROUND(
        AVG(CASE WHEN ca.channel_strategy <> 'Single-Channel' THEN ca.avg_clv END)
        OVER (PARTITION BY ca.customer_segment)
        -
        AVG(CASE WHEN ca.channel_strategy = 'Single-Channel' THEN ca.avg_clv END)
        OVER (PARTITION BY ca.customer_segment)
    , 2) AS multichannel_clv_premium,
    
    CASE
        WHEN ca.dominant_channel = 'Branch'
            THEN 'Traditional Channel (Baseline)'
        WHEN ca.dominant_channel IN ('Mobile', 'Online')
            THEN 'Digital Channel (Modernized)'
        ELSE 'Hybrid Channel'
    END AS channel_type,
    
    CURRENT_TIMESTAMP AS analyzed_at

FROM channel_aggregates ca
LEFT JOIN branch_baseline bb
    ON ca.customer_segment = bb.customer_segment

ORDER BY ca.customer_segment, ca.channel_strategy, ca.dominant_channel
