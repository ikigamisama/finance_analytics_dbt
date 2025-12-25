{{
    config(
        materialized='view',
        schema="gold",
        tags=['analytics', 'realtime', 'serving', 'accounts']
    )
}}

SELECT
    a.account_natural_key,
    c.customer_natural_key,
    c.customer_segment,
    p.product_name,
    
    a.account_status,
    ROUND(a.current_balance::numeric, 2) AS current_balance,
    ROUND(a.available_balance::numeric, 2) AS available_balance,
    ROUND(a.credit_limit::numeric, 2) AS credit_limit,
    ROUND(a.credit_utilization_pct::numeric, 2) AS credit_utilization_pct,
    
    -- Alert conditions
    a.is_past_due,
    a.is_near_limit,
    a.payment_due_date,
    
    -- Alert type
    CASE
        WHEN a.is_past_due AND a.payment_due_date < CURRENT_DATE - INTERVAL '30 days' THEN 'CRITICAL: 30+ Days Past Due'
        WHEN a.is_past_due AND a.payment_due_date < CURRENT_DATE - INTERVAL '7 days' THEN 'HIGH: 7+ Days Past Due'
        WHEN a.is_past_due THEN 'MEDIUM: Payment Past Due'
        WHEN a.is_near_limit THEN 'HIGH: Near Credit Limit'
        WHEN a.credit_utilization_pct > 80 THEN 'MEDIUM: High Credit Utilization'
        WHEN a.available_balance < 100 AND a.current_balance > 0 THEN 'LOW: Low Available Balance'
        ELSE 'INFO'
    END AS alert_type,
    
    -- Alert priority
    CASE
        WHEN a.is_past_due AND a.payment_due_date < CURRENT_DATE - INTERVAL '30 days' THEN 1
        WHEN a.is_near_limit OR (a.is_past_due AND a.payment_due_date < CURRENT_DATE - INTERVAL '7 days') THEN 2
        ELSE 3
    END AS alert_priority,
    
    -- Recommended action
    CASE
        WHEN a.is_past_due AND a.payment_due_date < CURRENT_DATE - INTERVAL '30 days' THEN 'Urgent Collections Contact'
        WHEN a.is_past_due THEN 'Payment Reminder'
        WHEN a.is_near_limit THEN 'Credit Limit Review'
        WHEN a.credit_utilization_pct > 80 THEN 'Balance Transfer Offer'
        ELSE 'Monitor'
    END AS recommended_action,
    
    -- Days until due (if applicable)
    CASE
        WHEN a.payment_due_date IS NOT NULL 
        THEN a.payment_due_date - CURRENT_DATE
        ELSE NULL
    END AS days_until_due,
    
    CURRENT_TIMESTAMP AS alert_time
    
FROM {{ ref('dim_account') }} a
INNER JOIN {{ ref('dim_customer') }} c ON a.customer_id = c.customer_natural_key
INNER JOIN {{ ref('dim_product') }} p ON a.product_id = p.product_natural_key
    
WHERE a.is_current = TRUE
  AND a.is_active = TRUE
  AND c.is_current = TRUE
  AND (
      a.is_past_due = TRUE
      OR a.is_near_limit = TRUE
      OR a.credit_utilization_pct > 80
      OR (a.available_balance < 100 AND a.current_balance > 0)
  )
  
ORDER BY alert_priority, current_balance DESC
LIMIT 100