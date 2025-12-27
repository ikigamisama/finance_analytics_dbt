{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'predictive', 'serving', 'fraud']
    )
}}

WITH fraud_patterns AS (
    SELECT
        merchant_category,
        channel,
        EXTRACT(HOUR FROM transaction_date) AS hour_of_day,
        is_international,
        is_weekend,
        
        COUNT(*) AS total_transactions,
        SUM(CAST(is_fraud_flag as INTEGER)) AS fraud_count,
        AVG(fraud_score) AS avg_fraud_score,
        STDDEV(fraud_score) AS stddev_fraud_score,
        
        -- Historical fraud rate
        ROUND(SUM(CAST(is_fraud_flag as INTEGER)) * 100.0 / COUNT(*), 2) AS historical_fraud_rate_pct
        
    FROM {{ ref('fact_transactions') }}
    GROUP BY merchant_category, channel, hour_of_day, is_international, is_weekend
),
predictions AS (
    SELECT
        *,
        ROUND(
            LEAST(100, GREATEST(0,
                historical_fraud_rate_pct *
                CASE
                    WHEN hour_of_day BETWEEN 22 AND 6 THEN 1.3
                    WHEN hour_of_day BETWEEN 9 AND 17 THEN 0.9
                    ELSE 1.0
                END *
                CASE WHEN is_international THEN 1.5 ELSE 1.0 END *
                CASE WHEN is_weekend THEN 1.2 ELSE 1.0 END *
                (1 + COALESCE(stddev_fraud_score, 0) * 0.5)
            ))::numeric
        , 2) AS predicted_fraud_risk_pct
    FROM fraud_patterns
)

SELECT
    *,
    CASE
        WHEN predicted_fraud_risk_pct >= 5 THEN 'Critical'
        WHEN predicted_fraud_risk_pct >= 2 THEN 'High'
        WHEN predicted_fraud_risk_pct >= 1 THEN 'Medium'
        ELSE 'Low'
    END AS predicted_risk_level,
    ROUND(
        total_transactions * 30.0 / 180 * (predicted_fraud_risk_pct / 100), 0
    ) AS expected_fraud_count_30d,
    ROUND(avg_fraud_score::numeric, 3) AS avg_historical_fraud_score,
    CURRENT_TIMESTAMP AS prediction_date
FROM predictions
WHERE total_transactions >= 100
ORDER BY predicted_fraud_risk_pct DESC