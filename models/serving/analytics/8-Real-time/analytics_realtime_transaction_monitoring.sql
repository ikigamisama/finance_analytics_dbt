{{
    config(
        materialized='view',
        schema='gold',
        tags=['analytics', 'realtime', 'serving', 'transactions']
    )
}}

WITH realtime_metrics AS (
    SELECT
        -- Current hour metrics
        COUNT(*) FILTER (
            WHERE transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
        ) AS transactions_current_hour,

        SUM(transaction_amount_abs) FILTER (
            WHERE transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
        ) AS volume_current_hour,

        -- Last 15 minutes
        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
        ) AS transactions_last_15min,

        SUM(transaction_amount_abs) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
        ) AS volume_last_15min,

        -- Last 5 minutes
        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ) AS transactions_last_5min,

        -- Alerts (Postgres-safe)
        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
              AND is_fraud_flag
        ) AS fraud_alerts_15min,

        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
              AND is_declined_flag  = 1
        ) AS declined_15min,

        -- Comparison to same hour yesterday
        COUNT(*) FILTER (
            WHERE transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP - INTERVAL '1 day')
              AND transaction_date <  DATE_TRUNC('hour', CURRENT_TIMESTAMP - INTERVAL '1 day')
                                   + INTERVAL '1 hour'
        ) AS transactions_same_hour_yesterday,

        -- Average transaction amount
        AVG(transaction_amount_abs) FILTER (
            WHERE transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
        ) AS avg_amount_current_hour,

        -- Unique customers
        COUNT(DISTINCT customer_key) FILTER (
            WHERE transaction_date >= DATE_TRUNC('hour', CURRENT_TIMESTAMP)
        ) AS unique_customers_current_hour,

        -- Channel breakdown (last 15 min)
        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
              AND channel = 'Mobile'
        ) AS mobile_15min,

        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
              AND channel = 'Online'
        ) AS online_15min,

        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
              AND channel = 'ATM'
        ) AS atm_15min,

        COUNT(*) FILTER (
            WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
              AND channel = 'Branch'
        ) AS branch_15min

    FROM {{ ref('fact_transactions') }}
    WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '25 hours'
)

SELECT
    -- Timestamp
    CURRENT_TIMESTAMP AS snapshot_time,
    DATE_TRUNC('hour', CURRENT_TIMESTAMP) AS current_hour,

    -- Current metrics
    transactions_current_hour,
    ROUND(volume_current_hour::numeric, 2) AS volume_current_hour,
    transactions_last_15min,
    ROUND(volume_last_15min::numeric, 2) AS volume_last_15min,
    transactions_last_5min,

    -- Rates (transactions per minute)
    ROUND((transactions_last_15min / 15.0)::numeric, 2) AS transactions_per_minute_15min,
    ROUND((transactions_last_5min / 5.0)::numeric, 2) AS transactions_per_minute_5min,

    -- Alerts
    fraud_alerts_15min,
    declined_15min,
    ROUND(
        (fraud_alerts_15min * 100.0 / NULLIF(transactions_last_15min, 0))::numeric,
        2
    ) AS fraud_rate_pct_15min,
    ROUND(
        (declined_15min * 100.0 / NULLIF(transactions_last_15min, 0))::numeric,
        2
    ) AS decline_rate_pct_15min,

    -- Average amounts
    ROUND(avg_amount_current_hour::numeric, 2) AS avg_amount_current_hour,

    -- Customer activity
    unique_customers_current_hour,
    ROUND(
        (transactions_current_hour * 1.0 / NULLIF(unique_customers_current_hour, 0))::numeric,
        2
    ) AS avg_transactions_per_customer,

    -- Comparison
    transactions_same_hour_yesterday,
    ROUND(
        ((transactions_current_hour - transactions_same_hour_yesterday)
            * 100.0 / NULLIF(transactions_same_hour_yesterday, 0))::numeric,
        2
    ) AS change_vs_yesterday_pct,

    -- Status indicator
    CASE
        WHEN transactions_current_hour < transactions_same_hour_yesterday * 0.5
            THEN 'ALERT: Low Volume'
        WHEN fraud_alerts_15min > 10
            THEN 'ALERT: High Fraud'
        WHEN declined_15min * 100.0 / NULLIF(transactions_last_15min, 0) > 5
            THEN 'ALERT: High Declines'
        WHEN transactions_current_hour > transactions_same_hour_yesterday * 1.5
            THEN 'INFO: High Volume'
        ELSE 'NORMAL'
    END AS system_status,

    -- Channel breakdown
    mobile_15min,
    online_15min,
    atm_15min,
    branch_15min,
    ROUND((mobile_15min * 100.0 / NULLIF(transactions_last_15min, 0))::numeric, 1) AS mobile_pct,
    ROUND((online_15min * 100.0 / NULLIF(transactions_last_15min, 0))::numeric, 1) AS online_pct

FROM realtime_metrics
