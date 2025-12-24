{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'transactions']
    )
}}

SELECT
    -- Volume Metrics
    COUNT(DISTINCT transaction_key) AS total_transactions,
    ROUND(SUM(transaction_amount_abs)::numeric, 2) AS total_transaction_volume,
    ROUND(AVG(transaction_amount_abs)::numeric, 2) AS avg_transaction_amount,
    
    -- By Direction
    COUNT(DISTINCT CASE WHEN transaction_direction = 'Debit' THEN transaction_key END) AS debit_count,
    COUNT(DISTINCT CASE WHEN transaction_direction = 'Credit' THEN transaction_key END) AS credit_count,
    ROUND(SUM(CASE WHEN transaction_direction = 'Debit' THEN transaction_amount_abs ELSE 0 END)::numeric, 2) AS total_debits,
    ROUND(SUM(CASE WHEN transaction_direction = 'Credit' THEN transaction_amount_abs ELSE 0 END)::numeric, 2) AS total_credits,
    
    -- By Channel
    COUNT(DISTINCT CASE WHEN channel = 'Online' THEN transaction_key END) AS online_transactions,
    COUNT(DISTINCT CASE WHEN channel = 'Mobile' THEN transaction_key END) AS mobile_transactions,
    COUNT(DISTINCT CASE WHEN channel = 'Branch' THEN transaction_key END) AS branch_transactions,
    COUNT(DISTINCT CASE WHEN channel = 'ATM' THEN transaction_key END) AS atm_transactions,
    
    -- Fraud Metrics
    COUNT(DISTINCT CASE WHEN is_fraud_flag THEN transaction_key END) AS fraud_transactions,
    ROUND(SUM(fraud_amount)::numeric, 2) AS total_fraud_amount,
    ROUND(COUNT(DISTINCT CASE WHEN is_fraud_flag THEN transaction_key END) * 100.0 / COUNT(DISTINCT transaction_key), 2) AS fraud_rate_pct,
    
    -- High Value
    COUNT(DISTINCT CASE WHEN is_high_value_flag THEN transaction_key END) AS high_value_transactions,
    
    -- International
    COUNT(DISTINCT CASE WHEN is_international THEN transaction_key END) AS international_transactions,
    
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_transactions') }}