{{
    config(
        materialized='view',
        schema="gold",
        tags=['analytics', 'realtime', 'serving', 'customer']
    )
}}

WITH recent_activity AS (
    SELECT
        t.customer_key,
        c.customer_natural_key,
        c.customer_segment,
        c.churn_risk_category,
        
        -- Transaction activity
        COUNT(*) AS transactions_last_hour,
        SUM(t.transaction_amount_abs) AS volume_last_hour,
        MAX(t.transaction_date) AS last_transaction_time,
        STRING_AGG(DISTINCT t.channel, ', ') AS channels_used,
        STRING_AGG(DISTINCT t.merchant_category, ', ') AS categories_used,
        
        -- Risk indicators
        SUM(CAST(t.is_fraud_flag AS INTEGER)) AS fraud_alerts,
        SUM(CAST(t.is_high_value_flag AS INTEGER)) AS high_value_transactions,
        SUM(CASE WHEN t.is_international THEN 1 ELSE 0 END) AS international_transactions,
        
        -- Unusual activity flags
        CASE 
            WHEN COUNT(*) > 10 THEN TRUE  -- Unusual frequency
            ELSE FALSE 
        END AS unusual_frequency,
        
        CASE
            WHEN SUM(t.transaction_amount_abs) > 10000 THEN TRUE  -- Unusual volume
            ELSE FALSE
        END AS unusual_volume
        
    FROM {{ ref('fact_transactions') }} t
    INNER JOIN {{ ref('dim_customer') }} c ON t.customer_key = c.customer_key
    WHERE t.transaction_date >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
      AND c.is_current = TRUE
    GROUP BY t.customer_key, c.customer_natural_key, c.customer_segment, c.churn_risk_category
)

SELECT
    customer_key,
    customer_natural_key,
    customer_segment,
    churn_risk_category,
    
    transactions_last_hour,
    ROUND(volume_last_hour::numeric, 2) AS volume_last_hour,
    last_transaction_time,
    ROUND((EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_transaction_time)) / 60)::numeric, 1) AS minutes_since_last_transaction,
    
    channels_used,
    categories_used,
    
    fraud_alerts,
    high_value_transactions,
    international_transactions,
    
    -- Activity level
    CASE
        WHEN transactions_last_hour >= 10 THEN 'Very High'
        WHEN transactions_last_hour >= 5 THEN 'High'
        WHEN transactions_last_hour >= 2 THEN 'Moderate'
        ELSE 'Low'
    END AS activity_level,
    
    -- Alert flags
    unusual_frequency,
    unusual_volume,
    
    -- Overall status
    CASE
        WHEN fraud_alerts > 0 THEN 'ALERT: Fraud Detected'
        WHEN unusual_frequency OR unusual_volume THEN 'ALERT: Unusual Activity'
        WHEN high_value_transactions > 0 THEN 'INFO: High Value Activity'
        ELSE 'NORMAL'
    END AS activity_status,
    
    CURRENT_TIMESTAMP AS snapshot_time
    
FROM recent_activity
WHERE transactions_last_hour > 0
ORDER BY 
    CASE 
        WHEN fraud_alerts > 0 THEN 1
        WHEN unusual_frequency OR unusual_volume THEN 2
        WHEN high_value_transactions > 0 THEN 3
        ELSE 4
    END,
    volume_last_hour DESC
LIMIT 200