{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'inferential', 'serving', 'customer']
    )
}}

WITH segment_metrics AS (
    SELECT
        c.customer_segment,
        c.customer_lifetime_value,
        c.annual_income,
        c.credit_score,
        c.tenure_months,
        c.churn_risk_score,
        
        COALESCE(t.transaction_count_90d, 0) AS transaction_count_90d,
        COALESCE(t.avg_transaction_amount, 0) AS avg_transaction_amount,
        COALESCE(a.total_balance, 0) AS total_balance
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS transaction_count_90d,
            AVG(transaction_amount_abs) AS avg_transaction_amount
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            SUM(current_balance) AS total_balance
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE AND is_active = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    
    WHERE c.is_current = TRUE AND c.is_active = TRUE
)

SELECT
    customer_segment,
    
    -- Sample size
    COUNT(*) AS sample_size,
    
    -- CLV statistics
    ROUND(AVG(customer_lifetime_value), 2) AS mean_clv,
    ROUND(STDDEV(customer_lifetime_value), 2) AS stddev_clv,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY customer_lifetime_value), 2) AS median_clv,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY customer_lifetime_value), 2) AS q1_clv,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY customer_lifetime_value), 2) AS q3_clv,
    
    -- 95% Confidence Interval for CLV
    ROUND(AVG(customer_lifetime_value) - 1.96 * STDDEV(customer_lifetime_value) / SQRT(COUNT(*)), 2) AS clv_ci_lower,
    ROUND(AVG(customer_lifetime_value) + 1.96 * STDDEV(customer_lifetime_value) / SQRT(COUNT(*)), 2) AS clv_ci_upper,
    
    -- Income statistics
    ROUND(AVG(annual_income), 2) AS mean_income,
    ROUND(STDDEV(annual_income), 2) AS stddev_income,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY annual_income), 2) AS median_income,
    
    -- Credit score statistics
    ROUND(AVG(credit_score), 0) AS mean_credit_score,
    ROUND(STDDEV(credit_score), 2) AS stddev_credit_score,
    
    -- Transaction statistics
    ROUND(AVG(transaction_count_90d), 2) AS mean_transactions,
    ROUND(STDDEV(transaction_count_90d), 2) AS stddev_transactions,
    
    -- Balance statistics
    ROUND(AVG(total_balance), 2) AS mean_balance,
    ROUND(STDDEV(total_balance), 2) AS stddev_balance,
    
    -- Churn risk statistics
    ROUND(AVG(churn_risk_score) * 100, 2) AS mean_churn_risk_pct,
    ROUND(STDDEV(churn_risk_score) * 100, 2) AS stddev_churn_risk_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM segment_metrics
GROUP BY customer_segment
ORDER BY mean_clv DESC