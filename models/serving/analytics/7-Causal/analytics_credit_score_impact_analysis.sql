{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'credit']
    )
}}

WITH credit_score_cohorts AS (
    SELECT
        c.customer_key,
        c.credit_score,
        c.credit_score_band,
        c.annual_income,
        c.customer_lifetime_value,
        c.tenure_months,
        
        -- Outcomes
        COALESCE(t.transaction_count, 0) AS transaction_count,
        COALESCE(t.total_volume, 0) AS total_transaction_volume,
        COALESCE(a.account_count, 0) AS account_count,
        COALESCE(a.total_balance, 0) AS total_balance,
        COALESCE(ca.approval_rate, 0) AS credit_approval_rate,
        COALESCE(lp.default_flag, 0) AS has_defaulted
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS transaction_count,
            SUM(transaction_amount_abs) AS total_volume
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '365 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            COUNT(*) AS account_count,
            SUM(current_balance) AS total_balance
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    LEFT JOIN (
        SELECT
            customer_key,
            AVG(approved_flag) AS approval_rate
        FROM {{ ref('fact_credit_applications') }}
        GROUP BY customer_key
    ) ca ON c.customer_key = ca.customer_key
    LEFT JOIN (
        SELECT
            customer_key,
            MAX(CASE WHEN missed_payment_flag = 1 THEN 1 ELSE 0 END) AS default_flag
        FROM {{ ref('fact_loan_payments') }}
        GROUP BY customer_key
    ) lp ON c.customer_key = lp.customer_key
    
    WHERE c.is_current = TRUE
)

SELECT
    credit_score_band,
    
    -- Sample size
    COUNT(*) AS customer_count,
    
    -- Credit score statistics
    ROUND(AVG(credit_score), 0) AS avg_credit_score,
    ROUND(STDDEV(credit_score), 2) AS stddev_credit_score,
    
    -- Causal outcomes (controlled for income)
    ROUND(AVG(transaction_count), 1) AS avg_transactions,
    ROUND(AVG(total_transaction_volume), 2) AS avg_transaction_volume,
    ROUND(AVG(account_count), 2) AS avg_accounts,
    ROUND(AVG(total_balance), 2) AS avg_balance,
    ROUND(AVG(credit_approval_rate) * 100, 2) AS avg_approval_rate_pct,
    ROUND(AVG(has_defaulted) * 100, 2) AS default_rate_pct,
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv,
    
    -- Marginal effects (difference from next lower band)
    ROUND(
        AVG(transaction_count) - 
        LAG(AVG(transaction_count)) OVER (ORDER BY avg_credit_score)
    , 1) AS marginal_effect_transactions,
    
    ROUND(
        AVG(total_balance) - 
        LAG(AVG(total_balance)) OVER (ORDER BY avg_credit_score)
    , 2) AS marginal_effect_balance,
    
    ROUND(
        AVG(credit_approval_rate) * 100 - 
        LAG(AVG(credit_approval_rate) * 100) OVER (ORDER BY avg_credit_score)
    , 2) AS marginal_effect_approval_pct,
    
    -- Causal interpretation: Change per 100 point credit score increase
    ROUND(
        (AVG(customer_lifetime_value) - LAG(AVG(customer_lifetime_value)) OVER (ORDER BY avg_credit_score)) /
        NULLIF((AVG(credit_score) - LAG(AVG(credit_score)) OVER (ORDER BY avg_credit_score)), 0) * 100
    , 2) AS clv_change_per_100_credit_points,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM credit_score_cohorts
GROUP BY credit_score_band
ORDER BY avg_credit_score