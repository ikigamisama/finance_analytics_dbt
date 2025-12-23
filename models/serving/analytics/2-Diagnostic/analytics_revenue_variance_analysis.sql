{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'revenue']
    )
}}

WITH monthly_revenue AS (
    SELECT
        d.year_month,
        p.category AS product_category,
        c.customer_segment,
        
        -- Transaction revenue (interchange fees assumption)
        SUM(t.transaction_amount_abs) * 0.02 AS estimated_transaction_revenue,
        COUNT(DISTINCT t.transaction_key) AS transaction_count,
        
        -- Account fees (from accounts snapshot)
        SUM(p.monthly_fee) AS account_fee_revenue,
        
        -- Interest income (simplified)
        SUM(CASE WHEN p.interest_rate > 0 THEN a.current_balance * p.interest_rate / 12 ELSE 0 END) AS interest_revenue
        
    FROM {{ ref('fact_transactions') }} t
    INNER JOIN {{ ref('dim_date') }} d ON t.date_key = d.date_key
    INNER JOIN {{ ref('dim_customer') }} c ON t.customer_key = c.customer_key
    INNER JOIN {{ ref('dim_account') }} a ON t.account_key = a.account_key
    INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    WHERE d.date_actual >= CURRENT_DATE - INTERVAL '12 months'
      AND c.is_current = TRUE AND a.is_current = TRUE
    GROUP BY d.year_month, p.category, c.customer_segment
)

SELECT
    year_month,
    product_category,
    customer_segment,
    
    ROUND(estimated_transaction_revenue, 2) AS transaction_revenue,
    ROUND(account_fee_revenue, 2) AS fee_revenue,
    ROUND(interest_revenue, 2) AS interest_revenue,
    ROUND(estimated_transaction_revenue + account_fee_revenue + interest_revenue, 2) AS total_revenue,
    
    transaction_count,
    
    -- Month-over-month variance
    ROUND(total_revenue - LAG(total_revenue) OVER (
        PARTITION BY product_category, customer_segment ORDER BY year_month
    ), 2) AS mom_revenue_change,
    
    ROUND((total_revenue - LAG(total_revenue) OVER (
        PARTITION BY product_category, customer_segment ORDER BY year_month
    )) * 100.0 / NULLIF(LAG(total_revenue) OVER (
        PARTITION BY product_category, customer_segment ORDER BY year_month
    ), 0), 2) AS mom_revenue_change_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM monthly_revenue
ORDER BY year_month DESC, total_revenue DESC
LIMIT 500