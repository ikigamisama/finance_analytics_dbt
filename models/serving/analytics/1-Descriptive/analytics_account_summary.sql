{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'descriptive', 'serving', 'accounts']
    )
}}

SELECT
    COUNT(DISTINCT account_key) AS total_accounts,
    COUNT(DISTINCT CASE WHEN is_active THEN account_key END) AS active_accounts,
    COUNT(DISTINCT CASE WHEN is_closed THEN account_key END) AS closed_accounts,
    COUNT(DISTINCT CASE WHEN is_dormant THEN account_key END) AS dormant_accounts,
    
    -- Balance Metrics
    ROUND(SUM(CASE WHEN is_active THEN current_balance ELSE 0 END), 2) AS total_active_balance,
    ROUND(AVG(CASE WHEN is_active THEN current_balance END), 2) AS avg_account_balance,
    ROUND(SUM(CASE WHEN is_active THEN available_balance ELSE 0 END), 2) AS total_available_balance,
    
    -- Credit Metrics
    ROUND(SUM(CASE WHEN is_active AND credit_limit IS NOT NULL THEN credit_limit ELSE 0 END), 2) AS total_credit_limit,
    ROUND(AVG(CASE WHEN is_active THEN credit_utilization_pct END), 2) AS avg_credit_utilization_pct,
    
    -- Risk Metrics
    COUNT(DISTINCT CASE WHEN is_past_due THEN account_key END) AS past_due_accounts,
    COUNT(DISTINCT CASE WHEN is_near_limit THEN account_key END) AS near_limit_accounts,
    ROUND(COUNT(DISTINCT CASE WHEN is_past_due THEN account_key END) * 100.0 / COUNT(DISTINCT CASE WHEN is_active THEN account_key END), 2) AS past_due_rate_pct,
    
    -- Age
    ROUND(AVG(CASE WHEN is_active THEN account_age_months END), 1) AS avg_account_age_months,
    
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('dim_account') }}
WHERE is_current = TRUE