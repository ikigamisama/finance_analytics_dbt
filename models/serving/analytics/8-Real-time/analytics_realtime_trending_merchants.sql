{{
    config(
        materialized='view',
        schema="gold",
        tags=['analytics', 'realtime', 'serving', 'merchants']
    )
}}

WITH merchant_activity AS (
    SELECT
        m.merchant_name,
        m.category,
        m.merchant_type,
        
        -- Current hour metrics
        COUNT(*) FILTER (WHERE t.transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)) AS transactions_current_hour,
        SUM(t.transaction_amount_abs) FILTER (WHERE t.transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)) AS volume_current_hour,
        
        -- Previous hour metrics
        COUNT(*) FILTER (WHERE t.transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP) - INTERVAL '1 hour'
                          AND t.transaction_date < DATE_TRUNC('hour', CURRENT_TIMESTAMP)) AS transactions_previous_hour,
        
        -- Last 15 minutes
        COUNT(*) FILTER (WHERE t.transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes') AS transactions_15min,
        
        COUNT(DISTINCT t.customer_key) FILTER (WHERE t.transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)) AS unique_customers_current_hour
        
    FROM {{ ref('fact_transactions') }} t
    INNER JOIN {{ ref('dim_merchant') }} m ON t.merchant_key = m.merchant_key
    WHERE t.transaction_date >= CURRENT_TIMESTAMP - INTERVAL '2 hours'
    GROUP BY m.merchant_name, m.category, m.merchant_type
)

SELECT
    merchant_name,
    category,
    merchant_type,
    
    transactions_current_hour,
    ROUND(volume_current_hour::numeric, 2) AS volume_current_hour,
    transactions_15min,
    unique_customers_current_hour,
    
    -- Growth metrics
    transactions_previous_hour,
    transactions_current_hour - transactions_previous_hour AS transaction_change,
    ROUND((transactions_current_hour - transactions_previous_hour) * 100.0 / NULLIF(transactions_previous_hour, 0), 2) AS growth_rate_pct,
    
    -- Velocity (transactions per minute in last 15 min)
    ROUND(transactions_15min / 15.0, 2) AS velocity_per_minute,
    
    -- Trending indicator
    CASE
        WHEN transactions_current_hour >= transactions_previous_hour * 2 THEN '🔥 Hot'
        WHEN transactions_current_hour >= transactions_previous_hour * 1.5 THEN '📈 Trending Up'
        WHEN transactions_current_hour <= transactions_previous_hour * 0.5 THEN '📉 Trending Down'
        ELSE '➡️ Stable'
    END AS trend_indicator,
    
    -- Rank
    ROW_NUMBER() OVER (ORDER BY transactions_current_hour DESC) AS activity_rank,
    
    CURRENT_TIMESTAMP AS snapshot_time
    
FROM merchant_activity
WHERE transactions_current_hour > 0
ORDER BY transactions_current_hour DESC
LIMIT 50