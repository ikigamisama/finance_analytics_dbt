{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'loans']
    )
}}

SELECT
    COUNT(DISTINCT payment_key) AS total_payments,
    COUNT(DISTINCT CASE WHEN full_payment_flag = 1 THEN payment_key END) AS full_payments,
    COUNT(DISTINCT CASE WHEN late_payment_flag = 1 THEN payment_key END) AS late_payments,
    COUNT(DISTINCT CASE WHEN missed_payment_flag = 1 THEN payment_key END) AS missed_payments,
    
    -- Financial
    ROUND(SUM(scheduled_amount)::numeric, 2) AS total_scheduled,
    ROUND(SUM(actual_amount)::numeric, 2) AS total_actual,
    ROUND(SUM(late_fee)::numeric, 2) AS total_late_fees,
    ROUND(SUM(outstanding_balance)::numeric, 2) AS total_outstanding,
    
    -- Rates
    ROUND(COUNT(DISTINCT CASE WHEN late_payment_flag = 1 THEN payment_key END) * 100.0 / COUNT(DISTINCT payment_key), 2) AS late_payment_rate_pct,
    ROUND(COUNT(DISTINCT CASE WHEN missed_payment_flag = 1 THEN payment_key END) * 100.0 / COUNT(DISTINCT payment_key), 2) AS missed_payment_rate_pct,
    ROUND(AVG(days_late), 1) AS avg_days_late,
    
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_loan_payments') }}