{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'inferential', 'serving', 'churn']
    )
}}

WITH customer_status AS (
    SELECT
        customer_segment,
        age_group,
        tenure_months,
        
        COUNT(*) AS total_customers,
        SUM(CASE WHEN NOT is_active THEN 1 ELSE 0 END) AS churned_customers,
        SUM(CASE WHEN churn_risk_category = 'High Risk' THEN 1 ELSE 0 END) AS high_risk_customers,
        
        AVG(churn_risk_score) AS mean_churn_score
        
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
    GROUP BY customer_segment, age_group, tenure_months
)

SELECT
    customer_segment,
    age_group,
    
    -- Tenure grouping
    CASE
        WHEN tenure_months < 6 THEN '0-6 months'
        WHEN tenure_months < 12 THEN '6-12 months'
        WHEN tenure_months < 24 THEN '12-24 months'
        ELSE '24+ months'
    END AS tenure_group,
    
    SUM(total_customers) AS total_customers,
    SUM(churned_customers) AS churned_customers,
    
    -- Churn rate
    ROUND(SUM(churned_customers) * 100.0 / SUM(total_customers), 2) AS churn_rate_pct,
    
    -- Standard error for proportion
    ROUND(SQRT(
        (SUM(churned_customers) * 1.0 / SUM(total_customers)) * 
        (1 - SUM(churned_customers) * 1.0 / SUM(total_customers)) / 
        SUM(total_customers)
    ) * 100, 2) AS churn_rate_se_pct,
    
    -- 95% Confidence Interval
    ROUND(GREATEST(0, 
        (SUM(churned_customers) * 100.0 / SUM(total_customers)) - 
        1.96 * SQRT(
            (SUM(churned_customers) * 1.0 / SUM(total_customers)) * 
            (1 - SUM(churned_customers) * 1.0 / SUM(total_customers)) / 
            SUM(total_customers)
        ) * 100
    ), 2) AS churn_rate_ci_lower,
    
    ROUND(LEAST(100,
        (SUM(churned_customers) * 100.0 / SUM(total_customers)) + 
        1.96 * SQRT(
            (SUM(churned_customers) * 1.0 / SUM(total_customers)) * 
            (1 - SUM(churned_customers) * 1.0 / SUM(total_customers)) / 
            SUM(total_customers)
        ) * 100
    ), 2) AS churn_rate_ci_upper,
    
    -- High risk proportion
    ROUND(SUM(high_risk_customers) * 100.0 / SUM(total_customers), 2) AS high_risk_pct,
    
    -- Mean churn score
    ROUND(AVG(mean_churn_score) * 100, 2) AS mean_churn_score_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM customer_status
GROUP BY customer_segment, age_group, tenure_group
HAVING SUM(total_customers) >= 30  -- Minimum for confidence interval validity
ORDER BY churn_rate_pct DESC