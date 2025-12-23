{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'serving', 'accounts']
    )
}}

WITH closed_accounts AS (
    SELECT
        a.account_key,
        a.account_age_months,
        a.balance_category,
        c.customer_segment,
        c.tenure_months AS customer_tenure_months,
        c.churn_risk_category,
        p.product_name,
        p.category AS product_category,
        p.monthly_fee,
        
        -- Transaction activity before closure
        COALESCE(t.transaction_count_90d, 0) AS transaction_count_90d,
        COALESCE(t.avg_transaction_amount, 0) AS avg_transaction_amount,
        
        -- Service interactions before closure
        COALESCE(i.interaction_count_90d, 0) AS interaction_count_90d,
        COALESCE(i.negative_interactions, 0) AS negative_interactions
        
    FROM {{ ref('dim_account') }} a
    INNER JOIN {{ ref('dim_customer') }} c 
        ON a.customer_id = c.customer_natural_key AND c.is_current = TRUE
    INNER JOIN {{ ref('dim_product') }} p 
        ON a.product_id = p.product_natural_key
    LEFT JOIN (
        SELECT
            account_key,
            COUNT(*) AS transaction_count_90d,
            AVG(transaction_amount_abs) AS avg_transaction_amount
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY account_key
    ) t ON a.account_key = t.account_key
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS interaction_count_90d,
            SUM(negative_sentiment_flag) AS negative_interactions
        FROM {{ ref('fact_customer_interactions') }}
        WHERE interaction_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY customer_key
    ) i ON c.customer_key = i.customer_key
    
    WHERE a.is_current = TRUE 
      AND a.is_closed = TRUE
      AND a.close_date >= CURRENT_DATE - INTERVAL '180 days'
)

SELECT
    product_category,
    customer_segment,
    churn_risk_category,
    
    -- Closure metrics
    COUNT(*) AS closed_accounts,
    ROUND(AVG(account_age_months), 1) AS avg_account_age_months,
    ROUND(AVG(customer_tenure_months), 1) AS avg_customer_tenure_months,
    
    -- Activity patterns
    ROUND(AVG(transaction_count_90d), 1) AS avg_transactions_before_closure,
    ROUND(AVG(interaction_count_90d), 1) AS avg_service_contacts,
    ROUND(AVG(negative_interactions), 1) AS avg_negative_interactions,
    
    -- Product characteristics
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee,
    
    -- Balance at closure
    COUNT(CASE WHEN balance_category = 'Zero' THEN 1 END) AS zero_balance_closures,
    COUNT(CASE WHEN balance_category = 'Negative' THEN 1 END) AS negative_balance_closures,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM closed_accounts
GROUP BY product_category, customer_segment, churn_risk_category
ORDER BY closed_accounts DESC