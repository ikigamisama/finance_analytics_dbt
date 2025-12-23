{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'predictive', 'serving', 'customer']
    )
}}

WITH customer_metrics AS (
    SELECT
        c.customer_key,
        c.customer_natural_key,
        c.customer_segment,
        c.tenure_months,
        c.customer_lifetime_value AS current_clv,
        c.annual_income,
        c.age,
        
        -- Transaction metrics (monthly average)
        COALESCE(t.monthly_avg_transactions, 0) AS monthly_avg_transactions,
        COALESCE(t.monthly_avg_volume, 0) AS monthly_avg_volume,
        
        -- Account metrics
        COALESCE(a.total_balance, 0) AS total_balance,
        COALESCE(a.account_count, 0) AS account_count,
        
        -- Growth indicators
        COALESCE(s.segment_upgrades, 0) AS segment_upgrades
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) * 1.0 / 
                NULLIF(EXTRACT(MONTH FROM AGE(MAX(transaction_date), MIN(transaction_date))), 0) 
                AS monthly_avg_transactions,
            SUM(transaction_amount_abs) * 1.0 / 
                NULLIF(EXTRACT(MONTH FROM AGE(MAX(transaction_date), MIN(transaction_date))), 0)
                AS monthly_avg_volume
        FROM {{ ref('fact_transactions') }}
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            SUM(current_balance) AS total_balance,
            COUNT(*) AS account_count
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE AND is_active = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    LEFT JOIN (
        SELECT
            customer_key,
            SUM(CASE WHEN tier_movement = 'Upgrade' THEN 1 ELSE 0 END) AS segment_upgrades
        FROM {{ ref('fact_customer_segment_history') }}
        GROUP BY customer_key
    ) s ON c.customer_key = s.customer_key
    
    WHERE c.is_current = TRUE AND c.is_active = TRUE
)

SELECT
    customer_key,
    customer_natural_key,
    customer_segment,
    tenure_months,
    ROUND(current_clv, 2) AS current_clv,
    
    -- Predicted future CLV (12-month horizon)
    ROUND(
        current_clv + 
        (monthly_avg_volume * 0.02 * 12) +  -- Estimated revenue from transactions
        (total_balance * 0.01) +  -- Asset-based revenue
        (account_count * 120) +  -- Account fee revenue
        (segment_upgrades * 500) +  -- Upgrade bonus
        
        -- Growth factor based on tenure
        CASE
            WHEN tenure_months < 12 THEN monthly_avg_volume * 0.03 * 12
            WHEN tenure_months < 24 THEN monthly_avg_volume * 0.02 * 12
            ELSE monthly_avg_volume * 0.01 * 12
        END
    , 2) AS predicted_clv_12m,
    
    -- 36-month projection
    ROUND(
        current_clv + 
        (monthly_avg_volume * 0.02 * 36) +
        (total_balance * 0.025) +
        (account_count * 360) +
        (segment_upgrades * 1500) +
        CASE
            WHEN tenure_months < 12 THEN monthly_avg_volume * 0.05 * 36
            WHEN tenure_months < 24 THEN monthly_avg_volume * 0.03 * 36
            ELSE monthly_avg_volume * 0.02 * 36
        END
    , 2) AS predicted_clv_36m,
    
    -- Value increase
    ROUND(predicted_clv_12m - current_clv, 2) AS expected_value_increase_12m,
    ROUND(predicted_clv_36m - current_clv, 2) AS expected_value_increase_36m,
    
    -- Growth rate
    ROUND((predicted_clv_12m - current_clv) * 100.0 / NULLIF(current_clv, 0), 2) AS growth_rate_12m_pct,
    
    -- Value tier prediction
    CASE
        WHEN predicted_clv_36m >= 50000 THEN 'High Value'
        WHEN predicted_clv_36m >= 25000 THEN 'Medium-High Value'
        WHEN predicted_clv_36m >= 10000 THEN 'Medium Value'
        ELSE 'Standard Value'
    END AS predicted_value_tier,
    
    -- Supporting metrics
    monthly_avg_transactions,
    ROUND(monthly_avg_volume, 2) AS monthly_avg_volume,
    account_count,
    ROUND(total_balance, 2) AS total_balance,
    
    CURRENT_TIMESTAMP AS prediction_date
    
FROM customer_metrics
ORDER BY predicted_clv_36m DESC