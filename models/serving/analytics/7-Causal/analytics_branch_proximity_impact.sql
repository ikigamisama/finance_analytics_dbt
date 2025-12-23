{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'geography']
    )
}}

WITH customer_branch_distance AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        -- Simplified distance calculation (assuming location data exists)
        CASE
            WHEN c.city = l.city THEN 'Same City'
            WHEN c.state = l.state THEN 'Same State'
            ELSE 'Different State'
        END AS proximity_category,
        
        -- Outcomes
        COUNT(DISTINCT t.transaction_key) FILTER (WHERE t.channel = 'Branch') AS branch_transactions,
        COUNT(DISTINCT t.transaction_key) FILTER (WHERE t.channel = 'Mobile') AS mobile_transactions,
        COUNT(DISTINCT t.transaction_key) AS total_transactions,
        SUM(a.current_balance) AS total_balance,
        
        c.tenure_months,
        c.churn_risk_score
        
    FROM {{ ref('dim_customer') }} c
    CROSS JOIN {{ ref('dim_location') }} l
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON c.customer_key = t.customer_key
        AND t.transaction_date >= CURRENT_DATE - INTERVAL '180 days'
    LEFT JOIN {{ ref('dim_account') }} a 
        ON c.customer_natural_key = a.customer_id 
        AND a.is_current = TRUE
    
    WHERE c.is_current = TRUE
      AND l.location_type = 'BRANCH'
      AND l.is_active = TRUE
    
    GROUP BY c.customer_key, c.customer_segment, c.city, c.state, 
             l.city, l.state, c.tenure_months, c.churn_risk_score
)

SELECT
    proximity_category,
    customer_segment,
    
    -- Sample size
    COUNT(DISTINCT customer_key) AS customer_count,
    
    -- Channel usage patterns
    ROUND(AVG(branch_transactions), 2) AS avg_branch_transactions,
    ROUND(AVG(mobile_transactions), 2) AS avg_mobile_transactions,
    ROUND(AVG(branch_transactions) * 100.0 / NULLIF(AVG(total_transactions), 0), 2) AS branch_usage_pct,
    
    -- Financial outcomes
    ROUND(AVG(total_balance), 2) AS avg_balance,
    ROUND(AVG(total_transactions), 2) AS avg_total_transactions,
    
    -- Retention outcomes
    ROUND(AVG(tenure_months), 1) AS avg_tenure_months,
    ROUND(AVG(churn_risk_score) * 100, 2) AS avg_churn_risk_pct,
    
    -- Causal effect (compared to "Different State" baseline)
    ROUND(
        AVG(branch_transactions) - 
        FIRST_VALUE(AVG(branch_transactions)) OVER (
            PARTITION BY customer_segment 
            ORDER BY CASE proximity_category 
                WHEN 'Same City' THEN 1 
                WHEN 'Same State' THEN 2 
                ELSE 3 
            END DESC
        )
    , 2) AS branch_usage_effect,
    
    ROUND(
        AVG(churn_risk_score) - 
        FIRST_VALUE(AVG(churn_risk_score)) OVER (
            PARTITION BY customer_segment 
            ORDER BY CASE proximity_category 
                WHEN 'Same City' THEN 1 
                WHEN 'Same State' THEN 2 
                ELSE 3 
            END DESC
        )
    , 4) AS churn_risk_effect,
    
    -- Causal interpretation
    CASE
        WHEN proximity_category = 'Same City' THEN 'Maximum Proximity Effect'
        WHEN proximity_category = 'Same State' THEN 'Moderate Proximity Effect'
        ELSE 'Baseline (No Proximity Effect)'
    END AS causal_interpretation,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM customer_branch_distance
GROUP BY proximity_category, customer_segment
ORDER BY customer_segment, 
    CASE proximity_category 
        WHEN 'Same City' THEN 1 
        WHEN 'Same State' THEN 2 
        ELSE 3 
    END