{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'customer']
    )
}}

SELECT
    -- Total Metrics
    COUNT(DISTINCT customer_key) AS total_customers,
    COUNT(DISTINCT CASE WHEN is_active THEN customer_key END) AS active_customers,
    COUNT(DISTINCT CASE WHEN NOT is_active THEN customer_key END) AS inactive_customers,
    
    -- Segmentation
    COUNT(DISTINCT CASE WHEN customer_segment = 'Premium' THEN customer_key END) AS premium_customers,
    COUNT(DISTINCT CASE WHEN customer_segment = 'Affluent' THEN customer_key END) AS affluent_customers,
    COUNT(DISTINCT CASE WHEN customer_segment = 'Mass Market' THEN customer_key END) AS mass_market_customers,
    
    -- Financial Metrics
    ROUND(AVG(customer_lifetime_value)::numeric, 2) AS avg_customer_lifetime_value,
    ROUND(SUM(customer_lifetime_value)::numeric, 2) AS total_customer_lifetime_value,
    ROUND(AVG(annual_income)::numeric, 2) AS avg_annual_income,
    ROUND(AVG(credit_score)::numeric, 0) AS avg_credit_score,
    
    -- Risk Metrics
    COUNT(DISTINCT CASE WHEN churn_risk_category = 'High Risk' THEN customer_key END) AS high_churn_risk_customers,
    ROUND(AVG(churn_risk_score)::numeric * 100, 2) AS avg_churn_risk_pct,
    
    -- Tenure
    ROUND(AVG(tenure_months)::numeric, 1) AS avg_tenure_months,
    
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('dim_customer') }}
WHERE is_current = TRUE