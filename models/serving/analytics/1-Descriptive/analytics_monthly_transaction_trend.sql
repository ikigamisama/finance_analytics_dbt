{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'transactions']
    )
}}

SELECT
    d.year,
    d.month,
    d.month_name,
    d.year_month,
    COUNT(DISTINCT t.transaction_key) AS transaction_count,
    ROUND(SUM(t.transaction_amount_abs)::numeric, 2) AS total_volume,
    ROUND(AVG(t.transaction_amount_abs)::numeric, 2) AS avg_amount,
    COUNT(DISTINCT t.customer_key) AS active_customers,
    COUNT(DISTINCT CASE WHEN t.is_fraud_flag = TRUE THEN t.transaction_key END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN t.is_fraud_flag = TRUE THEN t.transaction_key END) * 100.0 / COUNT(DISTINCT t.transaction_key), 2) AS fraud_rate_pct,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name, d.year_month
ORDER BY d.year DESC, d.month DESC
LIMIT 24