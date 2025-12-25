{{
    config(
        materialized='view',
        schema="gold",
        tags=['analytics', 'realtime', 'serving', 'operations']
    )
}}

WITH system_metrics AS (
    SELECT
        -- Transaction processing
        COUNT(*) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes') AS transactions_5min,
        COUNT(*) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes') AS transactions_15min,
        AVG(processing_time_ms) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes') AS avg_processing_time_ms,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_time_ms) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes') AS p95_processing_time_ms,
        
        -- Success rates
        COUNT(*) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes' AND is_declined_flag = 0) * 100.0 / 
            NULLIF(COUNT(*) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'), 0) AS success_rate_pct,
        
        -- Fraud detection
        COUNT(*) FILTER (WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes' AND fraud_score >= 0.7) AS high_risk_transactions,
        
        -- Channel health
        COUNT(DISTINCT CASE WHEN transaction_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes' AND channel = 'Mobile' THEN transaction_key END) AS mobile_active,
        COUNT(DISTINCT CASE WHEN transaction_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes' AND channel = 'Online' THEN transaction_key END) AS online_active,
        COUNT(DISTINCT CASE WHEN transaction_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes' AND channel = 'ATM' THEN transaction_key END) AS atm_active,
        COUNT(DISTINCT CASE WHEN transaction_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes' AND channel = 'Branch' THEN transaction_key END) AS branch_active
        
    FROM {{ ref('fact_transactions') }}
    WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
)

SELECT
    CURRENT_TIMESTAMP AS snapshot_time,
    
    -- Transaction throughput
    transactions_5min,
    ROUND(transactions_5min / 5.0, 2) AS transactions_per_minute,
    transactions_15min,
    
    -- Performance metrics
    ROUND(avg_processing_time_ms, 2) AS avg_processing_time_ms,
    ROUND(p95_processing_time_ms::numeric, 2) AS p95_processing_time_ms,
    
    -- System health indicators
    ROUND(success_rate_pct::numeric, 2) AS success_rate_pct,
    high_risk_transactions,
    
    -- Overall system status
    CASE
        WHEN success_rate_pct < 95 THEN 'DEGRADED: Low Success Rate'
        WHEN avg_processing_time_ms > 1000 THEN 'DEGRADED: Slow Processing'
        WHEN transactions_5min = 0 THEN 'CRITICAL: No Transactions'
        WHEN high_risk_transactions > 20 THEN 'WARNING: High Fraud Activity'
        ELSE 'HEALTHY'
    END AS system_status,
    
    -- Performance status
    CASE
        WHEN avg_processing_time_ms < 200 THEN 'Excellent'
        WHEN avg_processing_time_ms < 500 THEN 'Good'
        WHEN avg_processing_time_ms < 1000 THEN 'Fair'
        ELSE 'Poor'
    END AS performance_status,
    
    -- Channel availability
    CASE WHEN mobile_active > 0 THEN 'UP' ELSE 'DOWN' END AS mobile_status,
    CASE WHEN online_active > 0 THEN 'UP' ELSE 'DOWN' END AS online_status,
    CASE WHEN atm_active > 0 THEN 'UP' ELSE 'DOWN' END AS atm_status,
    CASE WHEN branch_active > 0 THEN 'UP' ELSE 'DOWN' END AS branch_status,
    
    mobile_active AS mobile_transactions_5min,
    online_active AS online_transactions_5min,
    atm_active AS atm_transactions_5min,
    branch_active AS branch_transactions_5min
    
FROM system_metrics