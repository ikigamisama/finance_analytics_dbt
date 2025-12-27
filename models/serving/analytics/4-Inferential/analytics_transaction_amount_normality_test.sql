{{
    config(
        materialized='table',
        schema='gold',
        tags=['analytics', 'inferential', 'serving', 'transactions']
    )
}}

WITH transaction_stats AS (
    SELECT
        merchant_category,
        channel,

        COUNT(*) AS sample_size,
        AVG(transaction_amount_abs) AS mean_amount,
        STDDEV(transaction_amount_abs) AS stddev_amount,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY transaction_amount_abs) AS median_amount,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY transaction_amount_abs) AS q1_amount,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY transaction_amount_abs) AS q3_amount,
        MIN(transaction_amount_abs) AS min_amount,
        MAX(transaction_amount_abs) AS max_amount

    FROM {{ ref('fact_transactions') }}
    WHERE transaction_date >= GREATEST(
        CURRENT_DATE - INTERVAL '90 days',
        (SELECT MIN(transaction_date) FROM {{ ref('fact_transactions') }})
    )
    GROUP BY merchant_category, channel
)

SELECT
    merchant_category,
    channel,
    sample_size,

    -- Central tendency
    ROUND(mean_amount::numeric, 2) AS mean_amount,
    ROUND(median_amount::numeric, 2) AS median_amount,
    ROUND(stddev_amount::numeric, 2) AS stddev_amount,

    -- Quartiles and range
    ROUND(q1_amount::numeric, 2) AS q1_amount,
    ROUND(q3_amount::numeric, 2) AS q3_amount,
    ROUND((q3_amount - q1_amount)::numeric, 2) AS iqr,
    ROUND(min_amount::numeric, 2) AS min_amount,
    ROUND(max_amount::numeric, 2) AS max_amount,

    -- Coefficient of variation (%)
    ROUND(
        (stddev_amount * 100.0 / NULLIF(mean_amount, 0))::numeric,
        2
    ) AS cv_pct,

    -- Skewness indicator (mean vs median)
    ROUND(
        ((mean_amount - median_amount) / NULLIF(stddev_amount, 0))::numeric,
        2
    ) AS skewness_indicator,

    -- Outlier detection bounds (1.5 * IQR)
    ROUND(
        (q1_amount - 1.5 * (q3_amount - q1_amount))::numeric,
        2
    ) AS lower_outlier_bound,
    ROUND(
        (q3_amount + 1.5 * (q3_amount - q1_amount))::numeric,
        2
    ) AS upper_outlier_bound,

    CURRENT_TIMESTAMP AS last_updated

FROM transaction_stats
ORDER BY sample_size DESC
