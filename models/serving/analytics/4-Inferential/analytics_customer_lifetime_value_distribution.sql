{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'inferential', 'serving', 'customer']
    )
}}

WITH clv_distribution AS (
    SELECT
        customer_lifetime_value,
        customer_segment,
        
        -- Create bins for histogram
        CASE
            WHEN customer_lifetime_value < 1000 THEN '0-1K'
            WHEN customer_lifetime_value < 5000 THEN '1K-5K'
            WHEN customer_lifetime_value < 10000 THEN '5K-10K'
            WHEN customer_lifetime_value < 25000 THEN '10K-25K'
            WHEN customer_lifetime_value < 50000 THEN '25K-50K'
            ELSE '50K+'
        END AS clv_bin
        
    FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE AND is_active = TRUE
)

SELECT
    clv_bin,
    customer_segment,
    
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY customer_segment), 2) AS pct_of_segment,
    
    -- Distribution statistics
    ROUND(AVG(customer_lifetime_value), 2) AS mean_clv,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY customer_lifetime_value), 2) AS median_clv,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY customer_lifetime_value), 2) AS q1_clv,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY customer_lifetime_value), 2) AS q3_clv,
    ROUND(MIN(customer_lifetime_value), 2) AS min_clv,
    ROUND(MAX(customer_lifetime_value), 2) AS max_clv,
    ROUND(STDDEV(customer_lifetime_value), 2) AS stddev_clv,
    
    -- Skewness indicator (simplified)
    ROUND((AVG(customer_lifetime_value) - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY customer_lifetime_value)) / NULLIF(STDDEV(customer_lifetime_value), 0), 2) AS skewness_indicator,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM clv_distribution
GROUP BY clv_bin, customer_segment
ORDER BY customer_segment, 
    CASE clv_bin
        WHEN '0-1K' THEN 1
        WHEN '1K-5K' THEN 2
        WHEN '5K-10K' THEN 3
        WHEN '10K-25K' THEN 4
        WHEN '25K-50K' THEN 5
        WHEN '50K+' THEN 6
    END