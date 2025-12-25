{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'customer']
    )
}}

WITH customer_behavior AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        c.age_group,
        c.income_bracket,
        c.tenure_months,
        
        -- Transaction behavior
        COUNT(DISTINCT t.transaction_key) AS transaction_count,
        ROUND(AVG(t.transaction_amount_abs)::numeric, 2) AS avg_transaction_amount,
        COUNT(DISTINCT t.merchant_category) AS unique_categories,
        COUNT(DISTINCT CASE WHEN t.channel = 'Mobile' THEN t.transaction_key END) AS mobile_transactions,
        COUNT(DISTINCT CASE WHEN t.is_weekend THEN t.transaction_key END) AS weekend_transactions,
        
        -- Account behavior
        COUNT(DISTINCT a.account_key) AS num_accounts,
        ROUND(SUM(a.current_balance)::numeric, 2) AS total_balance,
        
        -- Service behavior
        COALESCE(i.interaction_count, 0) AS service_interactions,
        COALESCE(i.avg_satisfaction, 0) AS avg_satisfaction
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON c.customer_key = t.customer_key
        AND t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
    LEFT JOIN {{ ref('dim_account') }} a 
        ON c.customer_natural_key = a.customer_id 
        AND a.is_current = TRUE AND a.is_active = TRUE
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS interaction_count,
            AVG(satisfaction_rating) AS avg_satisfaction
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE c.is_current = TRUE AND c.is_active = TRUE
    GROUP BY c.customer_key, c.customer_segment, c.age_group, c.income_bracket, 
             c.tenure_months, i.interaction_count, i.avg_satisfaction
)

SELECT
    customer_segment,
    age_group,
    income_bracket,
    
    -- Behavioral metrics
    COUNT(*) AS customer_count,
    ROUND(AVG(transaction_count)::numeric, 1) AS avg_transactions_90d,
    ROUND(AVG(avg_transaction_amount)::numeric, 2) AS avg_transaction_size,
    ROUND(AVG(unique_categories)::numeric, 1) AS avg_unique_categories,
    ROUND(AVG(num_accounts)::numeric, 1) AS avg_accounts,
    ROUND(AVG(total_balance)::numeric, 2) AS avg_total_balance,
    
    -- Channel preferences
    ROUND((AVG(mobile_transactions) * 100.0 / NULLIF(AVG(transaction_count), 0))::numeric, 2) AS mobile_usage_pct,
    ROUND((AVG(weekend_transactions) * 100.0 / NULLIF(AVG(transaction_count), 0))::numeric, 2) AS weekend_activity_pct,
    
    -- Service metrics
    ROUND(AVG(service_interactions)::numeric, 1) AS avg_service_contacts,
    ROUND(AVG(avg_satisfaction)::numeric, 2) AS avg_satisfaction_score,
    
    -- Engagement score (composite)
    ROUND(
        ((AVG(transaction_count) * 0.3 + 
         AVG(num_accounts) * 10 + 
         AVG(avg_satisfaction) * 5) / 3)::numeric, 2
    ) AS engagement_score,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM customer_behavior
GROUP BY customer_segment, age_group, income_bracket
ORDER BY customer_count DESC