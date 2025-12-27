{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'prescriptive', 'serving', 'customer']
    )
}}

WITH at_risk_customers AS (
    SELECT
        c.customer_key,
        c.customer_natural_key,
        c.customer_segment,
        c.churn_risk_score,
        c.customer_lifetime_value,
        c.tenure_months,
        CURRENT_DATE - c.last_login_date::DATE AS days_since_login,
        
        -- Activity metrics
        COALESCE(t.transaction_count_90d, 0) AS transaction_count_90d,
        COALESCE(t.avg_amount, 0) AS avg_transaction_amount,
        
        -- Account health
        COALESCE(a.active_accounts, 0) AS active_accounts,
        COALESCE(a.past_due_accounts, 0) AS past_due_accounts,
        COALESCE(a.total_balance, 0) AS total_balance,
        
        -- Service issues
        COALESCE(i.unresolved_issues, 0) AS unresolved_issues,
        COALESCE(i.negative_interactions, 0) AS negative_interactions,
        COALESCE(i.days_since_interaction, 999) AS days_since_interaction
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS transaction_count_90d,
            AVG(transaction_amount_abs) AS avg_amount
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_accounts,
            SUM(CASE WHEN is_past_due THEN 1 ELSE 0 END) AS past_due_accounts,
            SUM(current_balance) AS total_balance
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    LEFT JOIN (
        SELECT
            customer_key,
            SUM(CASE WHEN resolved_flag = 0 THEN 1 ELSE 0 END) AS unresolved_issues,
            SUM(negative_sentiment_flag) AS negative_interactions,
            MIN(CURRENT_DATE - interaction_date::DATE) AS days_since_interaction
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE c.is_current = TRUE 
      AND c.is_active = TRUE
      AND c.churn_risk_score >= 0.5
), 
scored_actions AS (
    SELECT
        customer_key,
        customer_natural_key,
        customer_segment,
        ROUND((churn_risk_score * 100)::numeric, 2) AS churn_risk_pct,
        ROUND(customer_lifetime_value::numeric, 2) AS clv_at_risk,

        CASE
            WHEN unresolved_issues > 0 THEN 'URGENT: Resolve Service Issues'
            WHEN past_due_accounts > 0 THEN 'URGENT: Payment Assistance Program'
            WHEN days_since_login > 90 THEN 'Re-engagement Campaign'
            WHEN transaction_count_90d = 0 THEN 'Win-Back Offer'
            WHEN active_accounts <= 1 THEN 'Cross-Sell Campaign'
            WHEN negative_interactions > 2 THEN 'Customer Success Outreach'
            ELSE 'Loyalty Program Enrollment'
        END AS recommended_action,

        CASE
            WHEN unresolved_issues > 0 OR past_due_accounts > 0 THEN 1
            WHEN churn_risk_score >= 0.7 AND customer_lifetime_value >= 10000 THEN 1
            WHEN churn_risk_score >= 0.7 THEN 2
            WHEN customer_lifetime_value >= 10000 THEN 2
            ELSE 3
        END AS action_priority,

        CASE
            WHEN unresolved_issues > 0 OR negative_interactions > 2 THEN 'Phone Call'
            WHEN days_since_login <= 30 THEN 'In-App Message'
            WHEN days_since_login <= 60 THEN 'Email + SMS'
            ELSE 'Direct Mail'
        END AS recommended_channel,

        -- ✅ MATERIALIZE estimated_success_rate_pct HERE
        ROUND(
            CASE
                WHEN unresolved_issues > 0 THEN 65
                WHEN past_due_accounts > 0 THEN 55
                WHEN transaction_count_90d = 0 THEN 35
                WHEN active_accounts <= 1 THEN 45
                WHEN negative_interactions > 2 THEN 50
                ELSE 40
            END *
            CASE
                WHEN tenure_months >= 24 THEN 1.2
                WHEN tenure_months >= 12 THEN 1.0
                ELSE 0.8
            END
        , 0) AS estimated_success_rate_pct,

        days_since_login,
        transaction_count_90d,
        active_accounts,
        past_due_accounts,
        unresolved_issues,
        negative_interactions,
        customer_lifetime_value,
        CURRENT_TIMESTAMP AS generated_at

    FROM at_risk_customers
)

SELECT
    *,
    -- ✅ Now this works
    ROUND(
        customer_lifetime_value * 0.5 *
        (estimated_success_rate_pct / 100.0) -
        CASE recommended_action
            WHEN 'URGENT: Resolve Service Issues' THEN 150
            WHEN 'URGENT: Payment Assistance Program' THEN 200
            WHEN 'Re-engagement Campaign' THEN 50
            WHEN 'Win-Back Offer' THEN 100
            WHEN 'Cross-Sell Campaign' THEN 75
            WHEN 'Customer Success Outreach' THEN 125
            ELSE 40
        END
    , 2) AS expected_roi,

    CASE action_priority
        WHEN 1 THEN 'Within 48 hours'
        WHEN 2 THEN 'Within 1 week'
        ELSE 'Within 2 weeks'
    END AS recommended_timeline

FROM scored_actions
ORDER BY action_priority, clv_at_risk DESC