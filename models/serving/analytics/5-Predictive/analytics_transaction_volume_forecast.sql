{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'predictive', 'serving', 'transactions']
    )
}}

WITH historical_daily AS (
    SELECT
        d.date_actual,
        d.day_of_week,
        d.is_weekend,
        d.month,
        COUNT(DISTINCT t.transaction_key) AS transaction_count,
        SUM(t.transaction_amount_abs) AS total_volume,
        AVG(t.transaction_amount_abs) AS avg_amount
    FROM {{ ref('fact_transactions') }} t
    INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
    WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY d.date_actual, d.day_of_week, d.is_weekend, d.month
),

moving_averages AS (
    SELECT
        date_actual,
        day_of_week,
        is_weekend,
        month,
        transaction_count,
        total_volume,
        
        -- 7-day moving average
        AVG(transaction_count) OVER (
            ORDER BY date_actual
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7d_count,
        
        -- 30-day moving average
        AVG(transaction_count) OVER (
            ORDER BY date_actual
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma_30d_count,
        
        -- Trend (simple linear)
        ROW_NUMBER() OVER (ORDER BY date_actual) AS day_number,
        
        -- Standard deviation for confidence intervals
        STDDEV(transaction_count) OVER (
            ORDER BY date_actual
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS stddev_30d
        
    FROM historical_daily
),

forecast_base AS (
    SELECT
        date_actual,
        day_of_week,
        is_weekend,
        month,
        transaction_count AS actual_count,
        total_volume AS actual_volume,
        ma_7d_count,
        ma_30d_count,
        stddev_30d,
        
        -- Simple trend-adjusted forecast
        ROUND(ma_30d_count * 
            (1 + (ma_7d_count - ma_30d_count) / NULLIF(ma_30d_count, 0) * 0.3)
        , 0) AS forecasted_count,
        
        day_number
        
    FROM moving_averages
    WHERE date_actual >= CURRENT_DATE - INTERVAL '90 days'
)

SELECT
    date_actual,
    day_of_week,
    CASE day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    is_weekend,
    month,
    
    actual_count,
    ROUND(actual_volume::numeric, 2) AS actual_volume,
    
    forecasted_count,
    ROUND(forecasted_count * (actual_volume::numeric / NULLIF(actual_count, 0)), 2) AS forecasted_volume,
    
    -- 95% Prediction interval
    ROUND(GREATEST(0, forecasted_count - 1.96 * stddev_30d), 0) AS forecast_lower_bound,
    ROUND(forecasted_count + 1.96 * stddev_30d, 0) AS forecast_upper_bound,
    
    -- Forecast error (for historical dates)
    CASE
        WHEN actual_count IS NOT NULL 
        THEN ROUND(ABS(forecasted_count - actual_count) * 100.0 / NULLIF(actual_count, 0), 2)
        ELSE NULL
    END AS forecast_error_pct,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM forecast_base
ORDER BY date_actual DESC