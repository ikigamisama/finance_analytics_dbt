{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'predictive', 'serving', 'loans']
    )
}}

WITH loan_features AS (
    SELECT
        lp.account_key,
        a.account_natural_key,
        c.customer_key,
        c.credit_score,
        c.credit_score_band,
        c.income_bracket,
        c.debt_to_income_ratio,
        
        -- Payment history features
        COUNT(*) AS total_payments,
        SUM(lp.late_payment_flag) AS late_payment_count,
        SUM(lp.missed_payment_flag) AS missed_payment_count,
        AVG(lp.days_late) AS avg_days_late,
        MAX(lp.days_late) AS max_days_late,
        SUM(lp.late_fee) AS total_late_fees,
        
        -- Recent payment behavior (last 6 payments)
        SUM(CASE WHEN lp.payment_count <= 6 THEN lp.late_payment_flag ELSE 0 END) AS recent_late_payments,
        
        -- Account metrics
        AVG(lp.outstanding_balance) AS avg_outstanding_balance,
        MAX(lp.outstanding_balance) AS max_outstanding_balance,
        
        -- Product info
        p.interest_rate_pct,
        p.rate_category
        
    FROM {{ ref('fact_loan_payments') }} lp
    INNER JOIN {{ ref('dim_account') }} a ON lp.account_key = a.account_key
    INNER JOIN {{ ref('dim_customer') }} c ON lp.customer_key = c.customer_key
    INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    WHERE a.is_current = TRUE AND c.is_current = TRUE
    GROUP BY lp.account_key, a.account_natural_key, c.customer_key, c.credit_score,
             c.credit_score_band, c.income_bracket, c.debt_to_income_ratio,
             p.interest_rate_pct, p.rate_category
)

SELECT
    account_key,
    account_natural_key,
    customer_key,
    credit_score_band,
    income_bracket,
    rate_category,
    
    -- Default prediction score (0-100)
    ROUND(
        LEAST(100, GREATEST(0,
            -- Credit score factor (inverse)
            CASE
                WHEN credit_score < 580 THEN 40
                WHEN credit_score < 670 THEN 25
                WHEN credit_score < 740 THEN 12
                ELSE 5
            END +
            
            -- Payment history factor
            CASE
                WHEN late_payment_count = 0 THEN 0
                WHEN late_payment_count <= 2 THEN 15
                WHEN late_payment_count <= 5 THEN 30
                ELSE 45
            END +
            
            -- Recent behavior factor (weighted more)
            recent_late_payments * 8 +
            
            -- Missed payment penalty
            missed_payment_count * 12 +
            
            -- Days late severity
            CASE
                WHEN max_days_late > 90 THEN 25
                WHEN max_days_late > 60 THEN 18
                WHEN max_days_late > 30 THEN 10
                ELSE 0
            END +
            
            -- Interest rate stress
            CASE
                WHEN interest_rate_pct > 15 THEN 10
                WHEN interest_rate_pct > 10 THEN 5
                ELSE 0
            END
        ))
    , 2) AS default_probability_pct,
    
    -- Risk classification
    CASE
        WHEN default_probability_pct >= 70 THEN 'Critical'
        WHEN default_probability_pct >= 50 THEN 'High'
        WHEN default_probability_pct >= 30 THEN 'Medium'
        ELSE 'Low'
    END AS default_risk_category,
    
    -- Supporting metrics
    total_payments,
    late_payment_count,
    missed_payment_count,
    ROUND(avg_days_late, 1) AS avg_days_late,
    max_days_late,
    ROUND(total_late_fees, 2) AS total_late_fees,
    ROUND(avg_outstanding_balance, 2) AS avg_outstanding_balance,
    
    -- Expected loss calculation (simplified)
    ROUND(avg_outstanding_balance * (default_probability_pct / 100) * 0.6, 2) AS expected_loss_amount,
    
    CURRENT_TIMESTAMP AS prediction_date
    
FROM loan_features
WHERE total_payments >= 3  -- Minimum payment history
ORDER BY default_probability_pct DESC