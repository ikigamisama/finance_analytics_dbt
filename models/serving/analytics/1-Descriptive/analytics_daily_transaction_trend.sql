{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'transactions']
    )
}}

SELECT
    d.date_actual AS transaction_date,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT t.transaction_key) AS transaction_count,
    ROUND(SUM(t.transaction_amount_abs)::numeric, 2) AS total_volume,
    ROUND(AVG(t.transaction_amount_abs)::numeric, 2) AS avg_amount,
    COUNT(DISTINCT t.customer_key) AS active_customers,
    COUNT(DISTINCT CASE WHEN t.is_fraud_flag = TRUE THEN t.transaction_key END) AS fraud_count,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_transactions') }} t
INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
GROUP BY d.date_actual, d.day_name, d.is_weekend
ORDER BY d.date_actual DESC
LIMIT 365