{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'loans']
    )
}}

WITH loan_performance AS (
    SELECT
        lp.account_key,
        c.customer_segment,
        c.credit_score_band,
        c.income_bracket,
        c.employment_status,
        c.home_ownership,
        p.product_name,
        p.rate_category,
        
        -- Payment behavior
        COUNT(*) AS total_payments,
        SUM(lp.late_payment_flag) AS late_payments,
        SUM(lp.missed_payment_flag) AS missed_payments,
        ROUND(AVG(lp.days_late), 1) AS avg_days_late,
        ROUND(SUM(lp.late_fee)::numeric, 2) AS total_late_fees,
        
        -- Financial metrics
        AVG(lp.outstanding_balance) AS avg_outstanding_balance,
        ROUND((SUM(lp.late_payment_flag) * 100.0 / COUNT(*))::numeric, 2) AS late_payment_rate,
        
        -- Risk classification
        CASE 
            WHEN SUM(lp.missed_payment_flag) > 0 THEN 'High Risk'
            WHEN SUM(lp.late_payment_flag) > 3 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS risk_level
        
    FROM {{ ref('fact_loan_payments') }} lp
    INNER JOIN {{ ref('dim_account') }} a ON lp.account_key = a.account_key
    INNER JOIN {{ ref('dim_customer') }} c ON lp.customer_key = c.customer_key
    INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    WHERE a.is_current = TRUE AND c.is_current = TRUE
    GROUP BY lp.account_key, c.customer_segment, c.credit_score_band, c.income_bracket,
             c.employment_status, c.home_ownership, p.product_name, p.rate_category
)

SELECT
    customer_segment,
    credit_score_band,
    income_bracket,
    employment_status,
    risk_level,
    
    COUNT(*) AS loan_count,
    ROUND(AVG(late_payment_rate)::numeric, 2) AS avg_late_payment_rate,
    ROUND(AVG(avg_days_late)::numeric, 1) AS avg_days_late,
    ROUND(AVG(total_late_fees)::numeric, 2) AS avg_late_fees,
    ROUND(AVG(avg_outstanding_balance)::numeric, 2) AS avg_outstanding,
    
    -- Risk indicators
    SUM(CASE WHEN late_payments > 0 THEN 1 ELSE 0 END) AS accounts_with_late_payments,
    SUM(CASE WHEN missed_payments > 0 THEN 1 ELSE 0 END) AS accounts_with_missed_payments,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM loan_performance
GROUP BY customer_segment, credit_score_band, income_bracket, employment_status, risk_level
ORDER BY avg_late_payment_rate DESC