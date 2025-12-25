{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'inferential', 'serving', 'credit']
    )
}}

WITH approval_analysis AS (
    SELECT
        c.credit_score_band,
        c.income_bracket,
        c.employment_status,
        ca.dti_category,
        
        COUNT(*) AS total_applications,
        SUM(ca.approved_flag) AS approved_applications,
        ROUND(SUM(ca.approved_flag) * 100.0 / COUNT(*), 2) AS approval_rate_pct,
        
        -- Statistics for proportion
        ROUND(SQRT(
            (SUM(ca.approved_flag) * 1.0 / COUNT(*)) * 
            (1 - SUM(ca.approved_flag) * 1.0 / COUNT(*)) / 
            COUNT(*)
        ) * 100, 2) AS approval_rate_se_pct,
        
        AVG(ca.credit_score_at_application) AS mean_credit_score,
        AVG(ca.annual_income) AS mean_income,
        AVG(ca.debt_to_income_ratio) AS mean_dti,
        AVG(ca.requested_amount) AS mean_requested_amount
        
    FROM {{ ref('fact_credit_applications') }} ca
    INNER JOIN {{ ref('dim_customer') }} c ON ca.customer_key = c.customer_key
    WHERE ca.application_date >= CURRENT_DATE - INTERVAL '365 days'
      AND c.is_current = TRUE
    GROUP BY c.credit_score_band, c.income_bracket, c.employment_status, ca.dti_category
)

SELECT
    credit_score_band,
    income_bracket,
    employment_status,
    dti_category,
    
    total_applications AS sample_size,
    approved_applications,
    approval_rate_pct,
    approval_rate_se_pct,
    
    -- 95% Confidence Interval for approval rate
    ROUND(GREATEST(0, approval_rate_pct - 1.96 * approval_rate_se_pct), 2) AS approval_rate_ci_lower,
    ROUND(LEAST(100, approval_rate_pct + 1.96 * approval_rate_se_pct), 2) AS approval_rate_ci_upper,
    
    -- Supporting metrics
    ROUND(mean_credit_score, 0) AS mean_credit_score,
    ROUND(mean_income, 2) AS mean_income,
    ROUND(mean_dti::numeric * 100, 2) AS mean_dti_pct,
    ROUND(mean_requested_amount, 2) AS mean_requested_amount,
    
    -- Odds calculation (for logistic regression interpretation)
    ROUND(approval_rate_pct / (100 - approval_rate_pct), 3) AS approval_odds,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM approval_analysis
WHERE total_applications >= 50 
ORDER BY approval_rate_pct DESC