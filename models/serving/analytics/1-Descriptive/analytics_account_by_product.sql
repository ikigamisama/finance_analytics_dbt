{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'accounts']
    )
}}

SELECT
    p.product_name,
    p.category,
    p.product_line,
    COUNT(DISTINCT a.account_key) AS account_count,
    ROUND(COUNT(DISTINCT a.account_key) * 100.0 / SUM(COUNT(DISTINCT a.account_key)) OVER (), 2) AS pct_of_total,
    COUNT(DISTINCT CASE WHEN a.is_active THEN a.account_key END) AS active_accounts,
    ROUND(SUM(CASE WHEN a.is_active THEN a.current_balance ELSE 0 END)::numeric, 2) AS total_balance,
    ROUND(AVG(CASE WHEN a.is_active THEN a.current_balance END)::numeric, 2) AS avg_balance,
    COUNT(DISTINCT a.customer_id) AS unique_customers,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('dim_account') }} a
INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
WHERE a.is_current = TRUE
GROUP BY p.product_name, p.category, p.product_line
ORDER BY account_count DESC