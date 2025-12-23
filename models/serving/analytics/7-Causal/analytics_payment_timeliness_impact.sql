{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'credit']
    )
}}

WITH payment_behavior AS (
    SELECT
        lp.account_key,
        c.customer_key,
        c.customer_segment,
        c.credit_score,
        c.credit_score_band,
        
        -- Payment metrics
        COUNT(*) AS total_payments,
        SUM(lp.late_payment_flag) AS late_payment_count,
        SUM(lp.missed_payment_flag) AS missed_payment_count,
        AVG(lp.days_late) AS avg_days_late,
        MAX(lp.days_late) AS max_days_late,
        
        -- Payment timeliness classification
        CASE
            WHEN SUM(lp.missed_payment_flag) > 0 THEN 'Has Missed Payments'
            WHEN SUM(lp.late_payment_flag) > 3 THEN 'Frequently Late'
            WHEN SUM(lp.late_payment_flag) BETWEEN 1 AND 3 THEN 'Occasionally Late'
            ELSE 'Always On Time'
        END AS payment_behavior_category,
        
        -- Outcome variables
        AVG(lp.outstanding_balance) AS avg_outstanding_balance,
        SUM(lp.late_fee) AS total_late_fees,
        
        -- Account health
        a.credit_utilization_pct,
        a.is_past_due
        
    FROM {{ ref('fact_loan_payments') }} lp
    INNER JOIN {{ ref('dim_account') }} a ON lp.account_key = a.account_key
    INNER JOIN {{ ref('dim_customer') }} c ON lp.customer_key = c.customer_key
    WHERE a.is_current = TRUE AND c.is_current = TRUE
    GROUP BY lp.account_key, c.customer_key, c.customer_segment, c.credit_score,
             c.credit_score_band, a.credit_utilization_pct, a.is_past_due
)

SELECT
    payment_behavior_category,
    credit_score_band,
    customer_segment,
    
    -- Sample size
    COUNT(*) AS account_count,
    
    -- Payment metrics
    ROUND(AVG(total_payments), 1) AS avg_payment_history_length,
    ROUND(AVG(late_payment_count), 2) AS avg_late_payments,
    ROUND(AVG(avg_days_late), 1) AS avg_days_late,
    
    -- Financial outcomes
    ROUND(AVG(avg_outstanding_balance), 2) AS avg_outstanding_balance,
    ROUND(AVG(total_late_fees), 2) AS avg_late_fees_paid,
    ROUND(AVG(credit_utilization_pct), 2) AS avg_credit_utilization_pct,
    
    -- Credit health outcomes
    ROUND(AVG(credit_score), 0) AS avg_credit_score,
    ROUND(SUM(CASE WHEN is_past_due THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS past_due_rate_pct,
    
    -- Causal effects (compared to "Always On Time" baseline)
    ROUND(
        AVG(credit_score) - 
        MAX(CASE WHEN payment_behavior_category = 'Always On Time' 
            THEN AVG(credit_score) OVER (PARTITION BY customer_segment) END)
    , 2) AS credit_score_effect_vs_ontime,
    
    ROUND(
        AVG(credit_utilization_pct) - 
        MAX(CASE WHEN payment_behavior_category = 'Always On Time' 
            THEN AVG(credit_utilization_pct) OVER (PARTITION BY customer_segment) END)
    , 2) AS utilization_effect_vs_ontime,
    
    -- Cost of late payments
    ROUND(
        AVG(total_late_fees) - 
        MAX(CASE WHEN payment_behavior_category = 'Always On Time' 
            THEN AVG(total_late_fees) OVER (PARTITION BY customer_segment) END)
    , 2) AS additional_fees_vs_ontime,
    
    -- Causal interpretation
    CASE payment_behavior_category
        WHEN 'Always On Time' THEN 'Baseline (Optimal Behavior)'
        WHEN 'Occasionally Late' THEN 'Minor Credit Impact'
        WHEN 'Frequently Late' THEN 'Moderate Credit Damage'
        WHEN 'Has Missed Payments' THEN 'Severe Credit Damage'
    END AS impact_classification,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM payment_behavior
GROUP BY payment_behavior_category, credit_score_band, customer_segment
ORDER BY customer_segment, 
    CASE payment_behavior_category
        WHEN 'Always On Time' THEN 1
        WHEN 'Occasionally Late' THEN 2
        WHEN 'Frequently Late' THEN 3
        WHEN 'Has Missed Payments' THEN 4
    END