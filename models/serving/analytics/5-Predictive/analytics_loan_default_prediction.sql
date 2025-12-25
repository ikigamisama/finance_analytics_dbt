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
             c.credit_score_band, c.income_bracket,
             p.interest_rate_pct, p.rate_category
),
default_calc AS (
    SELECT
        *,
        ROUND(
            LEAST(100, GREATEST(0,
                CASE
                    WHEN credit_score < 580 THEN 40
                    WHEN credit_score < 670 THEN 25
                    WHEN credit_score < 740 THEN 12
                    ELSE 5
                END +
                CASE
                    WHEN late_payment_count = 0 THEN 0
                    WHEN late_payment_count <= 2 THEN 15
                    WHEN late_payment_count <= 5 THEN 30
                    ELSE 45
                END +
                recent_late_payments * 8 +
                missed_payment_count * 12 +
                CASE
                    WHEN max_days_late > 90 THEN 25
                    WHEN max_days_late > 60 THEN 18
                    WHEN max_days_late > 30 THEN 10
                    ELSE 0
                END +
                CASE
                    WHEN interest_rate_pct > 15 THEN 10
                    WHEN interest_rate_pct > 10 THEN 5
                    ELSE 0
                END
            )), 2
        ) AS default_probability_pct
    FROM loan_features
)
SELECT
    *,
    CASE
        WHEN default_probability_pct >= 70 THEN 'Critical'
        WHEN default_probability_pct >= 50 THEN 'High'
        WHEN default_probability_pct >= 30 THEN 'Medium'
        ELSE 'Low'
    END AS default_risk_category
FROM default_calc
WHERE total_payments >= 3
ORDER BY default_probability_pct DESC