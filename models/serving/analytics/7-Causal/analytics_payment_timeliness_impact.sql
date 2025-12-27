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
),
baseline AS (
    SELECT
        customer_segment,
        AVG(credit_score) AS avg_credit_score_ontime,
        COALESCE(AVG(credit_utilization_pct), 0) AS avg_utilization_ontime,
        AVG(total_late_fees) AS avg_late_fees_ontime
    FROM payment_behavior
    WHERE payment_behavior_category = 'Always On Time'
    GROUP BY customer_segment
)

SELECT
    pb.payment_behavior_category,
    pb.credit_score_band,
    pb.customer_segment,
    
    COUNT(*) AS account_count,
    
    ROUND(AVG(total_payments)::numeric, 1) AS avg_payment_history_length,
    ROUND(AVG(late_payment_count)::numeric, 2) AS avg_late_payments,
    ROUND(AVG(avg_days_late)::numeric, 1) AS avg_days_late,
    
    ROUND(AVG(avg_outstanding_balance)::numeric, 2) AS avg_outstanding_balance,
    ROUND(AVG(total_late_fees)::numeric, 2) AS avg_late_fees_paid,
    ROUND(COALESCE(AVG(credit_utilization_pct), 0)::numeric, 2) AS avg_credit_utilization_pct,

    ROUND(AVG(credit_score)::numeric, 0) AS avg_credit_score,
    ROUND(SUM(CASE WHEN is_past_due THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS past_due_rate_pct,

    -- Causal effects vs baseline
    ROUND(AVG(credit_score) - b.avg_credit_score_ontime, 2) AS credit_score_effect_vs_ontime,
    ROUND(COALESCE(AVG(credit_utilization_pct), 0) - COALESCE(b.avg_utilization_ontime, 0), 2) AS utilization_effect_vs_ontime,
    ROUND((AVG(total_late_fees) - b.avg_late_fees_ontime)::numeric, 2) AS additional_fees_vs_ontime,
    
    CASE pb.payment_behavior_category
        WHEN 'Always On Time' THEN 'Baseline (Optimal Behavior)'
        WHEN 'Occasionally Late' THEN 'Minor Credit Impact'
        WHEN 'Frequently Late' THEN 'Moderate Credit Damage'
        WHEN 'Has Missed Payments' THEN 'Severe Credit Damage'
    END AS impact_classification,
    
    CURRENT_TIMESTAMP AS analyzed_at

FROM payment_behavior pb
LEFT JOIN baseline b
    ON pb.customer_segment = b.customer_segment
GROUP BY pb.payment_behavior_category, pb.credit_score_band, pb.customer_segment, 
         b.avg_credit_score_ontime, b.avg_utilization_ontime, b.avg_late_fees_ontime
ORDER BY customer_segment, 
    CASE pb.payment_behavior_category
        WHEN 'Always On Time' THEN 1
        WHEN 'Occasionally Late' THEN 2
        WHEN 'Frequently Late' THEN 3
        WHEN 'Has Missed Payments' THEN 4
    END