{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'predictive', 'serving', 'churn']
    )
}}

WITH customer_features AS (
    SELECT
        c.customer_key,
        c.customer_natural_key,
        c.customer_segment,
        c.tenure_months,
        c.churn_risk_score AS historical_churn_score,
        c.last_login_date,
        CURRENT_DATE - c.last_login_date::DATE AS days_since_login,
        c.customer_lifetime_value,
        
        -- Transaction features (last 90 days)
        COALESCE(t.transaction_count, 0) AS transaction_count_90d,
        COALESCE(t.avg_amount, 0) AS avg_transaction_amount,
        COALESCE(t.unique_merchants, 0) AS unique_merchants_90d,
        COALESCE(t.unique_categories, 0) AS unique_categories_90d,
        
        -- Account features
        COALESCE(a.account_count, 0) AS account_count,
        COALESCE(a.active_accounts, 0) AS active_account_count,
        COALESCE(a.avg_balance, 0) AS avg_account_balance,
        COALESCE(a.past_due_count, 0) AS past_due_account_count,
        
        -- Service features
        COALESCE(i.interaction_count, 0) AS service_interactions_180d,
        COALESCE(i.negative_count, 0) AS negative_interactions,
        COALESCE(i.unresolved_count, 0) AS unresolved_issues
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS transaction_count,
            AVG(transaction_amount_abs) AS avg_amount,
            COUNT(DISTINCT merchant_key) AS unique_merchants,
            COUNT(DISTINCT merchant_category) AS unique_categories
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            COUNT(*) AS account_count,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_accounts,
            AVG(current_balance) AS avg_balance,
            SUM(CASE WHEN is_past_due THEN 1 ELSE 0 END) AS past_due_count
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS interaction_count,
            SUM(negative_sentiment_flag) AS negative_count,
            SUM(CASE WHEN resolved_flag = 0 THEN 1 ELSE 0 END) AS unresolved_count
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE c.is_current = TRUE AND c.is_active = TRUE
), 

churn_scores AS (
    SELECT
        customer_key,
        customer_natural_key,
        customer_segment,
        tenure_months,
        days_since_login,
        transaction_count_90d,
        active_account_count,
        past_due_account_count,
        unresolved_issues,
        negative_interactions,
        customer_lifetime_value,
        historical_churn_score,

        ROUND(
            (LEAST(100, GREATEST(0,
                historical_churn_score * 30 +

                CASE
                    WHEN days_since_login > 90 THEN 0.25
                    WHEN days_since_login > 60 THEN 0.15
                    WHEN days_since_login > 30 THEN 0.08
                    ELSE 0
                END * 20 +

                CASE
                    WHEN transaction_count_90d = 0 THEN 0.30
                    WHEN transaction_count_90d < 5 THEN 0.20
                    WHEN transaction_count_90d < 15 THEN 0.10
                    ELSE 0
                END * 15 +

                CASE
                    WHEN active_account_count = 0 THEN 0.40
                    WHEN past_due_account_count > 0 THEN 0.25
                    ELSE 0
                END * 15 +

                CASE
                    WHEN unresolved_issues > 2 THEN 0.30
                    WHEN unresolved_issues > 0 THEN 0.15
                    WHEN negative_interactions > 3 THEN 0.20
                    ELSE 0
                END * 10 +

                CASE
                    WHEN unique_categories_90d < 2 THEN 0.15
                    WHEN unique_categories_90d < 4 THEN 0.08
                    ELSE 0
                END * 10
            )) * 100)::numeric
        , 2) AS predicted_churn_risk_pct

    FROM customer_features
)

SELECT
    customer_key,
    customer_natural_key,
    customer_segment,
    tenure_months,

    ROUND((historical_churn_score * 100)::numeric, 2) AS historical_churn_risk_pct,
    predicted_churn_risk_pct,

    CASE
        WHEN predicted_churn_risk_pct >= 70 THEN 'Critical'
        WHEN predicted_churn_risk_pct >= 50 THEN 'High'
        WHEN predicted_churn_risk_pct >= 30 THEN 'Medium'
        ELSE 'Low'
    END AS predicted_churn_category,

    days_since_login,
    transaction_count_90d,
    active_account_count,
    past_due_account_count,
    unresolved_issues,
    negative_interactions,

    ROUND(customer_lifetime_value, 2) AS clv_at_risk,

    CURRENT_TIMESTAMP AS prediction_date,
    CURRENT_DATE + INTERVAL '90 days' AS prediction_window_end

FROM churn_scores
ORDER BY predicted_churn_risk_pct DESC