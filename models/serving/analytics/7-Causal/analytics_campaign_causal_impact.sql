{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'marketing']
    )
}}

WITH campaign_exposure AS (
    SELECT
        mc.campaign_id,
        mc.campaign_name,
        mc.target_segment,
        mc.start_date,
        mc.end_date,
        mc.conversions,
        
        -- Pre-campaign metrics (30 days before)
        pre.avg_transactions_before,
        pre.avg_balance_before,
        pre.avg_clv_before,
        
        -- Post-campaign metrics (30 days after)
        post.avg_transactions_after,
        post.avg_balance_after,
        post.avg_clv_after,
        
        -- Control group (non-targeted segment)
        ctrl.avg_transactions_control,
        ctrl.avg_balance_control
        
    FROM {{ ref('fact_marketing_campaigns') }} mc
    LEFT JOIN (
        SELECT
            target_segment,
            AVG(transaction_count) AS avg_transactions_before,
            AVG(total_balance) AS avg_balance_before,
            AVG(customer_lifetime_value) AS avg_clv_before
        FROM (
            SELECT
                c.customer_segment AS target_segment,
                COUNT(DISTINCT t.transaction_key) AS transaction_count,
                SUM(a.current_balance) AS total_balance,
                c.customer_lifetime_value
            FROM {{ ref('dim_customer') }} c
            LEFT JOIN {{ ref('fact_transactions') }} t 
                ON c.customer_key = t.customer_key
                AND t.transaction_date BETWEEN CURRENT_DATE - INTERVAL '60 days' AND CURRENT_DATE - INTERVAL '30 days'
            LEFT JOIN {{ ref('dim_account') }} a 
                ON c.customer_natural_key = a.customer_id AND a.is_current = TRUE
            WHERE c.is_current = TRUE
            GROUP BY c.customer_segment, c.customer_lifetime_value
        ) pre_metrics
        GROUP BY target_segment
    ) pre ON mc.target_segment = pre.target_segment
    LEFT JOIN (
        SELECT
            target_segment,
            AVG(transaction_count) AS avg_transactions_after,
            AVG(total_balance) AS avg_balance_after,
            AVG(customer_lifetime_value) AS avg_clv_after
        FROM (
            SELECT
                c.customer_segment AS target_segment,
                COUNT(DISTINCT t.transaction_key) AS transaction_count,
                SUM(a.current_balance) AS total_balance,
                c.customer_lifetime_value
            FROM {{ ref('dim_customer') }} c
            LEFT JOIN {{ ref('fact_transactions') }} t 
                ON c.customer_key = t.customer_key
                AND t.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
            LEFT JOIN {{ ref('dim_account') }} a 
                ON c.customer_natural_key = a.customer_id AND a.is_current = TRUE
            WHERE c.is_current = TRUE
            GROUP BY c.customer_segment, c.customer_lifetime_value
        ) post_metrics
        GROUP BY target_segment
    ) post ON mc.target_segment = post.target_segment
    LEFT JOIN (
        SELECT
            AVG(transaction_count) AS avg_transactions_control,
            AVG(total_balance) AS avg_balance_control
        FROM (
            SELECT
                COUNT(DISTINCT t.transaction_key) AS transaction_count,
                SUM(a.current_balance) AS total_balance
            FROM {{ ref('dim_customer') }} c
            LEFT JOIN {{ ref('fact_transactions') }} t 
                ON c.customer_key = t.customer_key
                AND t.transaction_date >= CURRENT_DATE - INTERVAL '30 days'
            LEFT JOIN {{ ref('dim_account') }} a 
                ON c.customer_natural_key = a.customer_id AND a.is_current = TRUE
            WHERE c.is_current = TRUE
              AND c.customer_segment = 'Mass Market'  -- Control group
            GROUP BY c.customer_key
        ) ctrl_metrics
    ) ctrl ON TRUE
    
    WHERE mc.campaign_status = 'Completed'
      AND mc.end_date >= CURRENT_DATE - INTERVAL '60 days'
)

SELECT
    campaign_id,
    campaign_name,
    target_segment,
    start_date,
    end_date,
    conversions,
    
    -- Pre-post comparison (Treatment Effect)
    ROUND(avg_transactions_before::numeric, 2) AS avg_transactions_before,
    ROUND(avg_transactions_after::numeric, 2) AS avg_transactions_after,
    ROUND(avg_transactions_after::numeric - avg_transactions_before::numeric, 2) AS transaction_change,
    ROUND((avg_transactions_after::numeric - avg_transactions_before::numeric) * 100.0 / NULLIF(avg_transactions_before::numeric, 0), 2) AS transaction_change_pct,
    
    -- Difference-in-Differences estimate
    ROUND(
        (avg_transactions_after::numeric - avg_transactions_before::numeric) - 
        (avg_transactions_control::numeric - avg_transactions_before::numeric)
    , 2) AS did_estimate_transactions,
    
    -- Balance impact
    ROUND(avg_balance_after::numeric - avg_balance_before::numeric, 2) AS balance_change,
    ROUND((avg_balance_after::numeric - avg_balance_before::numeric) * 100.0 / NULLIF(avg_balance_before::numeric, 0), 2) AS balance_change_pct,
    
    -- CLV impact
    ROUND(avg_clv_after::numeric - avg_clv_before::numeric, 2) AS clv_change,
    
    -- Causal interpretation
    CASE
        WHEN did_estimate_transactions > 0 AND transaction_change > 0 THEN 'Positive Causal Impact'
        WHEN did_estimate_transactions < 0 THEN 'Negative Causal Impact'
        WHEN transaction_change > 0 THEN 'Correlation Only (Not Causal)'
        ELSE 'No Impact'
    END AS causal_interpretation,
    
    -- Statistical significance indicator (simplified)
    CASE
        WHEN ABS(did_estimate_transactions) > 2 * STDDEV(avg_transactions_before) OVER ()
        THEN 'Statistically Significant'
        ELSE 'Not Significant'
    END AS significance,
    
    CURRENT_TIMESTAMP AS analyzed_at
    
FROM campaign_exposure
ORDER BY ABS(did_estimate_transactions) DESC