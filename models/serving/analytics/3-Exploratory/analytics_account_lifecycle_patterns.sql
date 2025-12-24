{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'lifecycle']
    )
}}

WITH account_cohorts AS (
    SELECT
        DATE_TRUNC('month', a.open_date)::DATE AS cohort_month,
        a.account_key,
        a.account_age_months,
        a.is_active,
        a.is_closed,
        a.current_balance,
        p.category AS product_category,
        c.customer_segment,
        
        -- Transaction activity
        COALESCE(t.transaction_count, 0) AS lifetime_transactions,
        COALESCE(t.total_volume, 0) AS lifetime_volume
        
    FROM {{ ref('dim_account') }} a
    INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    INNER JOIN {{ ref('dim_customer') }} c 
        ON a.customer_id = c.customer_natural_key AND c.is_current = TRUE
    LEFT JOIN (
        SELECT
            account_key,
            COUNT(*) AS transaction_count,
            SUM(transaction_amount_abs) AS total_volume
        FROM {{ ref('fact_transactions') }}
        GROUP BY account_key
    ) t ON a.account_key = t.account_key
    
    WHERE a.is_current = TRUE
      AND a.open_date >= CURRENT_DATE - INTERVAL '36 months'
)

SELECT
    cohort_month,
    product_category,
    customer_segment,
    
    -- Age buckets
    CASE
        WHEN account_age_months < 3 THEN '0-3 months'
        WHEN account_age_months < 6 THEN '3-6 months'
        WHEN account_age_months < 12 THEN '6-12 months'
        WHEN account_age_months < 24 THEN '12-24 months'
        ELSE '24+ months'
    END AS age_bucket,
    
    COUNT(*) AS account_count,
    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_accounts,
    SUM(CASE WHEN is_closed THEN 1 ELSE 0 END) AS closed_accounts,
    ROUND(SUM(CASE WHEN is_active THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS retention_rate_pct,
    
    ROUND(AVG(current_balance)::numeric, 2) AS avg_balance,
    ROUND(AVG(lifetime_transactions)::numeric, 1) AS avg_lifetime_transactions,
    ROUND(AVG(lifetime_volume)::numeric, 2) AS avg_lifetime_volume,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM account_cohorts
GROUP BY cohort_month, product_category, customer_segment, age_bucket
ORDER BY cohort_month DESC, product_category, age_bucket