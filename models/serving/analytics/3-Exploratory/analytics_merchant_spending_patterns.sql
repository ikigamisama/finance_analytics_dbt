{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'merchants']
    )
}}

SELECT
    m.category_group,
    m.category,
    m.region,
    c.customer_segment,
    c.age_group,
    
    COUNT(DISTINCT t.transaction_key) AS transaction_count,
    COUNT(DISTINCT t.customer_key) AS unique_customers,
    COUNT(DISTINCT t.merchant_key) AS unique_merchants,
    ROUND(SUM(t.transaction_amount_abs), 2) AS total_spend,
    ROUND(AVG(t.transaction_amount_abs), 2) AS avg_transaction_amount,
    
    -- Frequency patterns
    ROUND(COUNT(DISTINCT t.transaction_key) * 1.0 / NULLIF(COUNT(DISTINCT t.customer_key), 0), 2) AS transactions_per_customer,
    
    -- Loyalty indicators
    ROUND(AVG(CASE WHEN t.is_recurring THEN 1.0 ELSE 0.0 END) * 100, 2) AS recurring_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_merchant') }} m ON t.merchant_key = m.merchant_key
INNER JOIN {{ ref('dim_customer') }} c ON t.customer_key = c.customer_key
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '180 days'
  AND c.is_current = TRUE
GROUP BY m.category_group, m.category, m.region, c.customer_segment, c.age_group
HAVING COUNT(DISTINCT t.transaction_key) >= 50
ORDER BY total_spend DESC
LIMIT 200