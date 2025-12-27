{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'prescriptive', 'serving', 'credit']
    )
}}

WITH credit_accounts AS (
    SELECT
        a.account_key,
        a.account_natural_key,
        c.customer_key,
        c.customer_segment,
        c.credit_score,
        c.annual_income,
        a.credit_limit,
        a.current_balance,
        a.credit_utilization_pct,
        a.account_age_months,
        a.is_past_due,
        
        COALESCE(lp.late_payment_count, 0) AS late_payments_12m,
        COALESCE(lp.missed_payment_count, 0) AS missed_payments_12m,
        
        COALESCE(t.avg_monthly_spend, 0) AS avg_monthly_spend,
        COALESCE(t.max_monthly_spend, 0) AS max_monthly_spend
        
    FROM {{ ref('dim_account') }} a
    INNER JOIN {{ ref('dim_customer') }} c 
        ON a.customer_id = c.customer_natural_key
       AND c.is_current = TRUE
    INNER JOIN {{ ref('dim_product') }} p 
        ON a.product_id = p.product_natural_key
    LEFT JOIN (
        SELECT
            account_key,
            SUM(late_payment_flag) AS late_payment_count,
            SUM(missed_payment_flag) AS missed_payment_count
        FROM {{ ref('fact_loan_payments') }}
        WHERE scheduled_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY account_key
    ) lp ON a.account_key = lp.account_key
    LEFT JOIN (
        SELECT
            account_key,
            AVG(monthly_spend) AS avg_monthly_spend,
            MAX(monthly_spend) AS max_monthly_spend
        FROM (
            SELECT
                account_key,
                DATE_TRUNC('month', transaction_date) AS month,
                SUM(transaction_amount_abs) AS monthly_spend
            FROM {{ ref('fact_transactions') }}
            WHERE transaction_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY account_key, DATE_TRUNC('month', transaction_date)
        ) monthly
        GROUP BY account_key
    ) t ON a.account_key = t.account_key
    
    WHERE a.is_current = TRUE 
      AND a.is_active = TRUE
      AND p.category = 'CREDIT'
),

limit_base AS (
    SELECT
        account_key,
        account_natural_key,
        customer_key,
        customer_segment,
        credit_score,
        annual_income,
        credit_limit::numeric AS current_limit,
        credit_utilization_pct::numeric AS current_utilization_pct,
        account_age_months,
        late_payments_12m,
        missed_payments_12m,
        avg_monthly_spend,
        max_monthly_spend,
        is_past_due
    FROM credit_accounts
),

limit_recommendations AS (
    SELECT
        *,
        
        ROUND(
            LEAST(
                -- Income-based cap
                annual_income * 0.25,
                
                -- Spend-based + risk-adjusted limit
                GREATEST(
                    current_limit,
                    max_monthly_spend * 2,
                    avg_monthly_spend * 3
                )
                *
                CASE
                    WHEN credit_score >= 800 THEN 1.5
                    WHEN credit_score >= 740 THEN 1.3
                    WHEN credit_score >= 670 THEN 1.1
                    WHEN credit_score >= 580 THEN 0.9
                    ELSE 0.7
                END
                *
                CASE
                    WHEN late_payments_12m = 0 AND missed_payments_12m = 0 THEN 1.2
                    WHEN late_payments_12m = 0 THEN 1.0
                    WHEN late_payments_12m <= 2 THEN 0.8
                    ELSE 0.6
                END
                *
                CASE
                    WHEN current_utilization_pct > 80 THEN 1.3
                    WHEN current_utilization_pct > 50 THEN 1.2
                    WHEN current_utilization_pct > 30 THEN 1.1
                    ELSE 1.0
                END
            )::numeric
        , 2) AS recommended_limit
        
    FROM limit_base
), 

final_recommendations AS (
    SELECT
        account_key,
        account_natural_key,
        customer_key,
        customer_segment,
        credit_score,
        
        ROUND(current_limit, 2) AS current_limit,
        recommended_limit,
        
        ROUND(recommended_limit - current_limit, 2) AS limit_change,
        ROUND((recommended_limit - current_limit) * 100.0 / NULLIF(current_limit, 0), 2)
            AS limit_change_pct,
        
        -- LOOSENED CRITERIA FOR MORE RESULTS
        CASE
            WHEN recommended_limit > current_limit * 1.15  -- Lowered from 1.2
                 AND late_payments_12m <= 1                -- Allow 1 late payment
                THEN 'Increase Limit'
            WHEN recommended_limit < current_limit * 0.85  -- Raised from 0.8
                 AND (late_payments_12m > 1 OR is_past_due)
                THEN 'Decrease Limit'
            WHEN ABS(recommended_limit - current_limit) / NULLIF(current_limit, 0) < 0.1
                THEN 'No Change'
            ELSE 'Review Required'
        END AS recommended_action,
        
        CASE
            WHEN current_utilization_pct > 70 AND late_payments_12m = 0
                THEN 'High utilization, good payment history'
            WHEN late_payments_12m > 2
                THEN 'Payment history concerns'
            WHEN avg_monthly_spend > current_limit * 0.8
                THEN 'Spending exceeds limit capacity'
            WHEN current_utilization_pct < 20 AND account_age_months < 12
                THEN 'Low utilization, new account'
            ELSE 'Standard adjustment'
        END AS adjustment_rationale,
        
        CASE
            WHEN late_payments_12m = 0 AND missed_payments_12m = 0 THEN 'Low Risk'
            WHEN late_payments_12m <= 2 THEN 'Medium Risk'
            ELSE 'High Risk'
        END AS risk_level,
        
        current_utilization_pct,
        ROUND(current_limit / NULLIF(recommended_limit, 0) * current_utilization_pct, 2)
            AS projected_utilization_pct,
        
        account_age_months,
        late_payments_12m,
        avg_monthly_spend,
        
        CURRENT_TIMESTAMP AS generated_at

    FROM limit_recommendations
)

SELECT *
FROM final_recommendations
WHERE recommended_action IN ('Increase Limit', 'Decrease Limit', 'Review Required')
ORDER BY
    CASE recommended_action
        WHEN 'Increase Limit' THEN 1
        WHEN 'Review Required' THEN 2
        WHEN 'Decrease Limit' THEN 3
    END,
    ABS(limit_change) DESC