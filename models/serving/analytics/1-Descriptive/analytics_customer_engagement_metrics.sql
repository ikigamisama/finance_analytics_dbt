{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving','engagement']
    )
}}

WITH customer_engagement AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        c.preferred_channel,
        c.tenure_months,
        
        -- Transaction engagement
        COUNT(DISTINCT t.transaction_key) AS transaction_count,
        COUNT(DISTINCT DATE_TRUNC('month', t.transaction_date)) AS active_months,
        COUNT(DISTINCT t.merchant_category) AS unique_categories,
        COUNT(DISTINCT t.channel) AS channels_used,
        
        -- Digital engagement
        COUNT(DISTINCT CASE WHEN t.channel IN ('Mobile', 'Online') THEN t.transaction_key END) AS digital_transactions,
        
        -- Service engagement
        COALESCE(i.interaction_count, 0) AS service_interactions,
        COALESCE(i.avg_satisfaction, 0) AS avg_satisfaction_rating,
        
        -- Product engagement
        COALESCE(a.active_accounts, 0) AS active_products
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON c.customer_key = t.customer_key
        AND t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS interaction_count,
            AVG(satisfaction_rating) AS avg_satisfaction
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            COUNT(*) AS active_accounts
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE AND is_active = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, c.customer_segment, c.preferred_channel, c.tenure_months,
             i.interaction_count, i.avg_satisfaction, a.active_accounts
)

SELECT
    customer_segment,
    
    -- Overall metrics
    COUNT(*) AS customer_count,
    
    -- Transaction engagement
    ROUND(AVG(transaction_count), 1) AS avg_transactions_90d,
    ROUND(AVG(active_months), 1) AS avg_active_months,
    ROUND(AVG(unique_categories), 1) AS avg_categories_used,
    ROUND(AVG(channels_used), 1) AS avg_channels_used,
    
    -- Digital adoption
    ROUND(AVG(digital_transactions) * 100.0 / NULLIF(AVG(transaction_count), 0), 2) AS digital_adoption_pct,
    
    -- Engagement score (composite metric: 0-100)
    ROUND(
        (AVG(transaction_count)::numeric * 0.3 +
         AVG(active_months)::numeric * 10 +
         AVG(unique_categories)::numeric * 5 +
         AVG(channels_used)::numeric * 15 +
         AVG(avg_satisfaction_rating)::numeric * 10 +
         AVG(active_products)::numeric * 10) / 1.5
    , 2) AS engagement_score,
    
    -- Engagement level classification
    CASE
        WHEN AVG(transaction_count) >= 20 AND AVG(channels_used) >= 2 THEN 'Highly Engaged'
        WHEN AVG(transaction_count) >= 10 THEN 'Moderately Engaged'
        WHEN AVG(transaction_count) >= 3 THEN 'Low Engagement'
        ELSE 'Inactive'
    END AS engagement_level,
    
    -- Service metrics
    ROUND(AVG(service_interactions)::numeric, 1) AS avg_service_interactions,
    ROUND(AVG(avg_satisfaction_rating)::numeric, 2) AS avg_satisfaction_score,
    
    -- Product holdings
    ROUND(AVG(active_products)::numeric, 1) AS avg_active_products,
    
    -- Multi-channel usage
    ROUND(SUM(CASE WHEN channels_used >= 2 THEN 1 ELSE 0 END)::numeric * 100.0 / COUNT(*), 2) AS multi_channel_users_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM customer_engagement
GROUP BY customer_segment
ORDER BY engagement_score DESC