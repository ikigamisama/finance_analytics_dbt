{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'product']
    )
}}

WITH customer_product_adoption AS (
    SELECT
        c.customer_key,
        c.customer_segment,
        c.tenure_months,
        c.customer_lifetime_value,
        
        -- Product holdings
        COUNT(DISTINCT a.account_key) AS product_count,
        STRING_AGG(DISTINCT p.category, ', ' ORDER BY p.category) AS product_categories,
        
        -- Behavior metrics (90 days)
        COALESCE(t.transaction_count, 0) AS transaction_count_90d,
        COALESCE(t.total_volume, 0) AS total_volume_90d,
        COALESCE(t.avg_amount, 0) AS avg_transaction_amount,
        
        -- Engagement
        COALESCE(i.interaction_count, 0) AS service_interactions,
        c.churn_risk_score
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('dim_account') }} a 
        ON c.customer_natural_key = a.customer_id 
        AND a.is_current = TRUE AND a.is_active = TRUE
    LEFT JOIN {{ ref('dim_product') }} p 
        ON a.product_id = p.product_natural_key
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS transaction_count,
            SUM(transaction_amount_abs) AS total_volume,
            AVG(transaction_amount_abs) AS avg_amount
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS interaction_count
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, c.customer_segment, c.tenure_months, 
             c.customer_lifetime_value, c.churn_risk_score,
             t.transaction_count, t.total_volume, t.avg_amount, i.interaction_count
)

SELECT
    product_count,
    customer_segment,
    
    COUNT(*) AS customer_count,
    
    -- Outcome variables
    ROUND(AVG(customer_lifetime_value)::numeric, 2) AS avg_clv,
    ROUND(AVG(transaction_count_90d)::numeric, 1) AS avg_transactions,
    ROUND(AVG(total_volume_90d)::numeric, 2) AS avg_volume,
    ROUND((AVG(churn_risk_score) * 100)::numeric, 2) AS avg_churn_risk_pct,
    
    -- Causal effect (compared to 1 product baseline)
    ROUND(
        (AVG(customer_lifetime_value) - 
        FIRST_VALUE(AVG(customer_lifetime_value)) OVER (
            PARTITION BY customer_segment 
            ORDER BY product_count
        ))::numeric
    , 2) AS clv_effect_vs_baseline,
    
    ROUND(
        (AVG(transaction_count_90d) - 
        FIRST_VALUE(AVG(transaction_count_90d)) OVER (
            PARTITION BY customer_segment 
            ORDER BY product_count
        ))::numeric
    , 2) AS transaction_effect_vs_baseline,
    
    ROUND(
        (AVG(churn_risk_score) - 
        FIRST_VALUE(AVG(churn_risk_score)) OVER (
            PARTITION BY customer_segment 
            ORDER BY product_count
        ))::numeric
    , 4) AS churn_risk_effect_vs_baseline,
    
    -- Marginal effects (per additional product)
    ROUND(
       ( AVG(customer_lifetime_value) - 
        LAG(AVG(customer_lifetime_value)) OVER (
            PARTITION BY customer_segment 
            ORDER BY product_count
        ))::numeric
    , 2) AS marginal_clv_effect,
    
    -- Causal interpretation
    CASE
        WHEN product_count = 1 THEN 'Baseline (1 Product)'
        WHEN product_count = 2 THEN 'Cross-Sell Effect (2 Products)'
        WHEN product_count >= 3 THEN 'Multi-Product Effect (3+ Products)'
    END AS adoption_level,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM customer_product_adoption
GROUP BY product_count, customer_segment
ORDER BY customer_segment, product_count