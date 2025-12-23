{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'products']
    )
}}

WITH customer_products AS (
    SELECT
        a.customer_id,
        p.product_name,
        p.category,
        p.product_line,
        a.account_age_months,
        a.current_balance
    FROM {{ ref('dim_account') }} a
    INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    WHERE a.is_current = TRUE AND a.is_active = TRUE
),

product_combinations AS (
    SELECT
        p1.product_name AS product_1,
        p1.category AS category_1,
        p2.product_name AS product_2,
        p2.category AS category_2,
        COUNT(DISTINCT p1.customer_id) AS customer_count,
        ROUND(AVG(p1.current_balance + p2.current_balance), 2) AS avg_combined_balance,
        ROUND(AVG(LEAST(p1.account_age_months, p2.account_age_months)), 1) AS avg_min_age_months
    FROM customer_products p1
    INNER JOIN customer_products p2 
        ON p1.customer_id = p2.customer_id 
        AND p1.product_name < p2.product_name  -- Avoid duplicates
    GROUP BY p1.product_name, p1.category, p2.product_name, p2.category
)

SELECT
    product_1,
    category_1,
    product_2,
    category_2,
    customer_count,
    avg_combined_balance,
    avg_min_age_months,
    
    -- Cross-sell strength score
    ROUND(customer_count * LOG(avg_combined_balance + 1), 2) AS cross_sell_score,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM product_combinations
WHERE customer_count >= 10  -- Minimum threshold
ORDER BY customer_count DESC
LIMIT 100