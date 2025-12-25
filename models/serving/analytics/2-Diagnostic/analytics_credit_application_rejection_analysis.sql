{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'credit']
    )
}}

SELECT
    ca.decision,
    ca.risk_grade,
    ca.dti_category,
    c.credit_score_band,
    c.income_bracket,
    c.employment_status,
    p.product_name,
    
    COUNT(*) AS application_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_applications,
    
    -- Application metrics
    ROUND(AVG(ca.requested_amount)::numeric, 2) AS avg_requested_amount,
    ROUND(AVG(ca.credit_score_at_application)::numeric, 0) AS avg_credit_score,
    ROUND((AVG(ca.debt_to_income_ratio) * 100)::numeric, 2) AS avg_dti_pct,
    ROUND(AVG(ca.annual_income)::numeric, 2) AS avg_annual_income,
    ROUND((AVG(ca.approval_probability_score) * 100)::numeric, 2) AS avg_approval_probability_pct,
    
    -- Approval metrics
    SUM(ca.approved_flag) AS approved_count,
    ROUND(SUM(ca.approved_flag) * 100.0 / COUNT(*), 2) AS approval_rate_pct,
    ROUND(AVG(CASE WHEN ca.approved_flag = 1 THEN ca.approved_amount END)::numeric, 2) AS avg_approved_amount,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_credit_applications') }} ca
INNER JOIN {{ ref('dim_customer') }} c ON ca.customer_key = c.customer_key
INNER JOIN {{ ref('dim_product') }} p ON ca.product_key = p.product_key
WHERE ca.application_date >= CURRENT_DATE - INTERVAL '365 days'
  AND c.is_current = TRUE
GROUP BY ca.decision, ca.risk_grade, ca.dti_category, c.credit_score_band, 
         c.income_bracket, c.employment_status, p.product_name
ORDER BY application_count DESC