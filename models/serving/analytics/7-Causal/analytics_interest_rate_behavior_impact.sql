{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'pricing']
    )
}}

WITH rate_cohorts AS (
    SELECT
        a.account_key,
        p.interest_rate_pct,
        p.rate_category,
        a.account_age_months,
        a.current_balance,
        
        -- Outcomes
        COALESCE(t.transaction_count, 0) AS transaction_count,
        COALESCE(t.avg_amount, 0) AS avg_transaction_amount,
        COALESCE(lp.late_payment_rate, 0) AS late_payment_rate,
        a.is_past_due,
        
        c.churn_risk_score
        
    FROM {{ ref('dim_account') }} a
    INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    INNER JOIN {{ ref('dim_customer') }} c ON a.customer_id = c.customer_natural_key
    LEFT JOIN (
        SELECT
            account_key,
            COUNT(*) AS transaction_count,
            AVG(transaction_amount_abs) AS avg_amount
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '180 days'
        GROUP BY account_key
    ) t ON a.account_key = t.account_key
    LEFT JOIN (
        SELECT
            account_key,
            AVG(late_payment_flag) AS late_payment_rate
        FROM {{ ref('fact_loan_payments') }}
        GROUP BY account_key
    ) lp ON a.account_key = lp.account_key
    
    WHERE a.is_current = TRUE 
      AND c.is_current = TRUE
      AND p.interest_rate_pct > 0
)

SELECT
    rate_category,
    
    -- Sample statistics
    COUNT(*) AS account_count,
    ROUND(AVG(interest_rate_pct), 2) AS avg_interest_rate,
    ROUND(STDDEV(interest_rate_pct), 2) AS stddev_rate,
    
    -- Outcome variables
    ROUND(AVG(current_balance), 2) AS avg_balance,
    ROUND(AVG(transaction_count), 1) AS avg_transactions,
    ROUND(AVG(avg_transaction_amount), 2) AS avg_transaction_size,
    ROUND(AVG(late_payment_rate) * 100, 2) AS avg_late_payment_rate_pct,
    ROUND(AVG(CASE WHEN is_past_due THEN 1 ELSE 0 END) * 100, 2) AS past_due_rate_pct,
    ROUND(AVG(churn_risk_score) * 100, 2) AS avg_churn_risk_pct,
    
    -- Marginal effects (1% rate increase impact)
    ROUND(
        (AVG(current_balance) - LAG(AVG(current_balance)) OVER (ORDER BY avg_interest_rate)) /
        NULLIF((AVG(interest_rate_pct) - LAG(AVG(interest_rate_pct)) OVER (ORDER BY avg_interest_rate)), 0)
    , 2) AS balance_elasticity,
    
    ROUND(
        (AVG(late_payment_rate) - LAG(AVG(late_payment_rate)) OVER (ORDER BY avg_interest_rate)) * 100 /
        NULLIF((AVG(interest_rate_pct) - LAG(AVG(interest_rate_pct)) OVER (ORDER BY avg_interest_rate)), 0)
    , 2) AS late_payment_elasticity_pct,
    
    ROUND(
        (AVG(churn_risk_score) - LAG(AVG(churn_risk_score)) OVER (ORDER BY avg_interest_rate)) * 100 /
        NULLIF((AVG(interest_rate_pct) - LAG(AVG(interest_rate_pct)) OVER (ORDER BY avg_interest_rate)), 0)
    , 2) AS churn_elasticity_pct,
    
    -- Causal interpretation
    CASE
        WHEN balance_elasticity < -100 THEN 'High Negative Impact on Balances'
        WHEN balance_elasticity < 0 THEN 'Moderate Negative Impact on Balances'
        WHEN balance_elasticity > 0 THEN 'Positive or No Impact'
        ELSE 'Inconclusive'
    END AS balance_impact_interpretation,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM rate_cohorts
GROUP BY rate_category
ORDER BY avg_interest_rate