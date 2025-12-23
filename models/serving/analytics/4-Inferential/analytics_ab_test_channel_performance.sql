{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'inferential', 'serving', 'channels']
    )
}}

WITH channel_metrics AS (
    SELECT
        channel,
        COUNT(*) AS transaction_count,
        SUM(transaction_amount_abs) AS total_volume,
        AVG(transaction_amount_abs) AS mean_amount,
        STDDEV(transaction_amount_abs) AS stddev_amount,
        SUM(is_fraud_flag) AS fraud_count,
        SUM(is_declined_flag) AS declined_count,
        AVG(processing_time_ms) AS mean_processing_time,
        
        -- Conversion-like metrics
        COUNT(DISTINCT customer_key) AS unique_customers,
        COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT customer_key), 0) AS transactions_per_customer
        
    FROM {{ ref('fact_transactions') }}
    WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY channel
)

SELECT
    channel,
    transaction_count AS sample_size,
    
    -- Transaction amount statistics
    ROUND(mean_amount, 2) AS mean_transaction_amount,
    ROUND(stddev_amount, 2) AS stddev_transaction_amount,
    
    -- 95% Confidence Interval for mean amount
    ROUND(mean_amount - 1.96 * stddev_amount / SQRT(transaction_count), 2) AS amount_ci_lower,
    ROUND(mean_amount + 1.96 * stddev_amount / SQRT(transaction_count), 2) AS amount_ci_upper,
    
    -- Conversion rate (fraud rate as proxy)
    ROUND(fraud_count * 100.0 / transaction_count, 2) AS fraud_rate_pct,
    ROUND(SQRT(fraud_count * (1 - fraud_count * 1.0 / transaction_count) / transaction_count) * 196, 2) AS fraud_rate_se,
    
    -- Decline rate
    ROUND(declined_count * 100.0 / transaction_count, 2) AS decline_rate_pct,
    
    -- Processing time
    ROUND(mean_processing_time, 2) AS mean_processing_ms,
    
    -- Customer engagement
    ROUND(transactions_per_customer, 2) AS avg_transactions_per_customer,
    unique_customers,
    
    -- Total volume
    ROUND(total_volume, 2) AS total_volume,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM channel_metrics
ORDER BY transaction_count DESC