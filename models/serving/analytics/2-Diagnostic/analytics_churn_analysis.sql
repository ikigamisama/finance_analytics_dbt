{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'churn']
    )
}}

WITH churned_customers AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        c.tenure_months,
        c.churn_risk_score,
        c.last_login_date,
        c.customer_lifetime_value,
        c.credit_score,
        c.annual_income,
        CURRENT_DATE - c.last_login_date::DATE AS days_since_last_login,
        
        -- Account metrics
        COUNT(DISTINCT a.account_key) AS num_accounts,
        SUM(CASE WHEN a.is_closed THEN 1 ELSE 0 END) AS closed_accounts,
        
        -- Transaction activity
        COALESCE(t.transaction_count, 0) AS recent_transaction_count,
        COALESCE(t.avg_transaction_amount, 0) AS avg_transaction_amount,
        
        -- Interaction history
        COALESCE(i.interaction_count, 0) AS support_interactions,
        COALESCE(i.negative_interactions, 0) AS negative_interactions,
        COALESCE(i.unresolved_issues, 0) AS unresolved_issues
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('dim_account') }} a 
        ON c.customer_natural_key = a.customer_id AND a.is_current = TRUE
    LEFT JOIN (
        SELECT 
            customer_key,
            COUNT(*) AS transaction_count,
            AVG(transaction_amount_abs) AS avg_transaction_amount
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS interaction_count,
            SUM(CASE WHEN negative_sentiment_flag = 1 THEN 1 ELSE 0 END) AS negative_interactions,
            SUM(CASE WHEN resolved_flag = 0 THEN 1 ELSE 0 END) AS unresolved_issues
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE c.is_current = TRUE
      AND (c.churn_risk_score >= 0.6 OR NOT c.is_active)
    GROUP BY c.customer_key, c.customer_segment, c.tenure_months, c.churn_risk_score,
             c.last_login_date, c.customer_lifetime_value, c.credit_score, c.annual_income,
             t.transaction_count, t.avg_transaction_amount, i.interaction_count, 
             i.negative_interactions, i.unresolved_issues
)

SELECT
    customer_segment,
    
    -- Churn Indicators
    COUNT(*) AS at_risk_customers,
    ROUND(AVG(churn_risk_score) * 100, 2) AS avg_churn_risk_pct,
    ROUND(AVG(days_since_last_login), 1) AS avg_days_inactive,
    
    -- Account Health
    ROUND(AVG(num_accounts), 1) AS avg_accounts_per_customer,
    ROUND(AVG(closed_accounts), 1) AS avg_closed_accounts,
    
    -- Engagement
    ROUND(AVG(recent_transaction_count), 1) AS avg_recent_transactions,
    ROUND(AVG(avg_transaction_amount), 2) AS avg_transaction_size,
    
    -- Service Issues
    ROUND(AVG(support_interactions), 1) AS avg_support_contacts,
    ROUND(AVG(negative_interactions), 1) AS avg_negative_interactions,
    ROUND(AVG(unresolved_issues), 1) AS avg_unresolved_issues,
    
    -- Financial
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv,
    ROUND(SUM(customer_lifetime_value), 2) AS total_clv_at_risk,
    
    CURRENT_TIMESTAMP AS last_updated
FROM churned_customers
GROUP BY customer_segment
ORDER BY at_risk_customers DESC