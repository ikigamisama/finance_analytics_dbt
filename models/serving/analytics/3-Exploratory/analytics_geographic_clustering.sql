{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'exploratory', 'serving', 'geography']
    )
}}

SELECT
    c.state,
    c.city,
    
    -- Customer demographics
    COUNT(DISTINCT c.customer_key) AS customer_count,
    ROUND(AVG(c.credit_score), 0) AS avg_credit_score,
    ROUND(AVG(c.annual_income), 2) AS avg_annual_income,
    ROUND(AVG(c.customer_lifetime_value), 2) AS avg_clv,
    
    -- Distribution by segment
    ROUND(COUNT(DISTINCT CASE WHEN c.customer_segment = 'Premium' THEN c.customer_key END) * 100.0 / COUNT(DISTINCT c.customer_key), 2) AS premium_pct,
    ROUND(COUNT(DISTINCT CASE WHEN c.customer_segment = 'Affluent' THEN c.customer_key END) * 100.0 / COUNT(DISTINCT c.customer_key), 2) AS affluent_pct,
    
    -- Transaction activity
    COALESCE(t.total_transactions, 0) AS total_transactions_90d,
    COALESCE(ROUND(t.total_volume::numeric, 2), 0) AS total_volume_90d,
    COALESCE(ROUND(t.avg_transaction::numeric, 2), 0) AS avg_transaction_amount,
    
    -- Account metrics
    COALESCE(a.total_accounts, 0) AS total_accounts,
    COALESCE(ROUND(a.total_balance::numeric, 2), 0) AS total_balance,
    COALESCE(ROUND(a.avg_balance::numeric, 2), 0) AS avg_balance_per_account,
    
    -- Risk indicators
    ROUND((AVG(c.churn_risk_score))::numeric * 100, 2) AS avg_churn_risk_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('dim_customer') }} c
LEFT JOIN (
    SELECT
        c2.state,
        c2.city,
        COUNT(*) AS total_transactions,
        SUM(t.transaction_amount_abs) AS total_volume,
        AVG(t.transaction_amount_abs) AS avg_transaction
    FROM {{ ref('fact_transactions') }} t
    INNER JOIN {{ ref('dim_customer') }} c2 ON t.customer_key = c2.customer_key
    WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
      AND c2.is_current = TRUE
    GROUP BY c2.state, c2.city
) t ON c.state = t.state AND c.city = t.city
LEFT JOIN (
    SELECT
        c3.state,
        c3.city,
        COUNT(*) AS total_accounts,
        SUM(a.current_balance) AS total_balance,
        AVG(a.current_balance) AS avg_balance
    FROM {{ ref('dim_account') }} a
    INNER JOIN {{ ref('dim_customer') }} c3 ON a.customer_id = c3.customer_natural_key
    WHERE a.is_current = TRUE AND a.is_active = TRUE AND c3.is_current = TRUE
    GROUP BY c3.state, c3.city
) a ON c.state = a.state AND c.city = a.city

WHERE c.is_current = TRUE
GROUP BY c.state, c.city, t.total_transactions, t.total_volume, t.avg_transaction,
         a.total_accounts, a.total_balance, a.avg_balance
ORDER BY customer_count DESC
LIMIT 100