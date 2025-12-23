{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'transactions']
    )
}}

SELECT
    channel,
    COUNT(DISTINCT transaction_key) AS transaction_count,
    ROUND(COUNT(DISTINCT transaction_key) * 100.0 / SUM(COUNT(DISTINCT transaction_key)) OVER (), 2) AS pct_of_total,
    ROUND(SUM(transaction_amount_abs), 2) AS total_volume,
    ROUND(AVG(transaction_amount_abs), 2) AS avg_amount,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT(DISTINCT CASE WHEN is_fraud_flag = 1 THEN transaction_key END) AS fraud_count,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_transactions') }}
GROUP BY channel
ORDER BY transaction_count DESC