{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'credit']
    )
}}

WITH customer_metrics AS (
    SELECT
        c.customer_key,
        c.credit_score,
        c.credit_score_band,
        c.annual_income,
        c.income_bracket,
        c.customer_segment,
        c.tenure_months,
        
        -- Transaction behavior
        COUNT(DISTINCT t.transaction_key) AS transaction_count_90d,
        ROUND(AVG(t.transaction_amount_abs), 2) AS avg_transaction_amount,
        ROUND(STDDEV(t.transaction_amount_abs), 2) AS transaction_amount_volatility,
        
        -- Account health
        SUM(CASE WHEN a.is_past_due THEN 1 ELSE 0 END) AS past_due_accounts,
        ROUND(AVG(a.credit_utilization_pct), 2) AS avg_credit_utilization,
        ROUND(SUM(a.current_balance), 2) AS total_balance,
        
        -- Payment behavior
        COALESCE(lp.late_payment_count, 0) AS late_payments_12m,
        COALESCE(lp.missed_payment_count, 0) AS missed_payments_12m
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON c.customer_key = t.customer_key
        AND t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
    LEFT JOIN {{ ref('dim_account') }} a 
        ON c.customer_natural_key = a.customer_id 
        AND a.is_current = TRUE
    LEFT JOIN (
        SELECT
            customer_key,
            SUM(late_payment_flag) AS late_payment_count,
            SUM(missed_payment_flag) AS missed_payment_count
        FROM {{ ref('fact_loan_payments') }}
        WHERE scheduled_date >= CURRENT_DATE - INTERVAL '365 days'
        GROUP BY customer_key
    ) lp ON c.customer_key = lp.customer_key
    
    WHERE c.is_current = TRUE AND c.is_active = TRUE
    GROUP BY c.customer_key, c.credit_score, c.credit_score_band, c.annual_income,
             c.income_bracket, c.customer_segment, c.tenure_months,
             lp.late_payment_count, lp.missed_payment_count
)

SELECT
    credit_score_band,
    income_bracket,
    customer_segment,
    
    COUNT(*) AS customer_count,
    ROUND(AVG(credit_score), 0) AS avg_credit_score,
    ROUND(AVG(annual_income), 2) AS avg_annual_income,
    ROUND(AVG(transaction_count_90d), 1) AS avg_transactions,
    ROUND(AVG(avg_transaction_amount), 2) AS avg_transaction_size,
    ROUND(AVG(transaction_amount_volatility), 2) AS avg_volatility,
    ROUND(AVG(avg_credit_utilization), 2) AS avg_credit_utilization_pct,
    ROUND(AVG(total_balance), 2) AS avg_total_balance,
    
    -- Risk indicators
    ROUND(AVG(past_due_accounts), 2) AS avg_past_due_accounts,
    ROUND(AVG(late_payments_12m), 2) AS avg_late_payments,
    ROUND(AVG(missed_payments_12m), 2) AS avg_missed_payments,
    
    -- Correlation indicators
    ROUND(AVG(tenure_months), 1) AS avg_tenure_months,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM customer_metrics
GROUP BY credit_score_band, income_bracket, customer_segment
ORDER BY avg_credit_score DESC