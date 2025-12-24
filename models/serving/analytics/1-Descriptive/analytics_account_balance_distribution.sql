{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'accounts']
    )
}}

SELECT
    balance_category,
    COUNT(DISTINCT account_key) AS account_count,
    ROUND(COUNT(DISTINCT account_key) * 100.0 / SUM(COUNT(DISTINCT account_key)) OVER (), 2) AS pct_of_total,
    ROUND(SUM(current_balance)::numeric, 2) AS total_balance,
    ROUND(AVG(current_balance)::numeric, 2) AS avg_balance,
    ROUND(MIN(current_balance)::numeric, 2) AS min_balance,
    ROUND(MAX(current_balance)::numeric, 2) AS max_balance,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('dim_account') }}
WHERE is_current = TRUE AND is_active = TRUE
GROUP BY balance_category
ORDER BY 
    CASE balance_category
        WHEN 'Negative' THEN 1
        WHEN 'Zero' THEN 2
        WHEN 'Low' THEN 3
        WHEN 'Medium' THEN 4
        WHEN 'High' THEN 5
        WHEN 'Very High' THEN 6
    END