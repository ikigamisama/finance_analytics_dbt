{{
    config(
        materialized='incremental',
        schema="gold",
        unique_key=['account_key', 'snapshot_date_key'],
        tags=['gold', 'fact', 'serving', 'account_snapshot']
    )
}}

WITH daily_snapshots AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['a.account_id']) }} AS account_key,
        {{ dbt_utils.generate_surrogate_key(['CURRENT_DATE']) }} AS snapshot_date_key,
        a.customer_id,
        {{ dbt_utils.generate_surrogate_key(['a.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['a.product_id']) }} AS product_key,
        
        -- Snapshot Date
        CURRENT_DATE AS snapshot_date,
        
        -- Balance Measures
        a.current_balance,
        a.available_balance,
        a.credit_limit,
        a.credit_utilization_pct,
        
        -- Status Measures
        a.account_age_months,
        CASE WHEN a.is_active THEN 1 ELSE 0 END AS active_account_count,
        CASE WHEN a.is_closed THEN 1 ELSE 0 END AS closed_account_count,
        CASE WHEN a.is_dormant THEN 1 ELSE 0 END AS dormant_account_count,
        CASE WHEN a.is_past_due THEN 1 ELSE 0 END AS past_due_count,
        CASE WHEN a.is_near_limit THEN 1 ELSE 0 END AS near_limit_count,
        
        -- Transaction Activity (from transactions)
        COALESCE(t.daily_transaction_count, 0) AS daily_transaction_count,
        COALESCE(t.daily_transaction_amount, 0) AS daily_transaction_amount,
        COALESCE(t.daily_debit_count, 0) AS daily_debit_count,
        COALESCE(t.daily_credit_count, 0) AS daily_credit_count,
        
        1 AS account_count,
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_accounts') }} a
    LEFT JOIN (
        SELECT
            account_id,
            COUNT(*) AS daily_transaction_count,
            SUM(amount) AS daily_transaction_amount,
            SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) AS daily_debit_count,
            SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) AS daily_credit_count
        FROM {{ ref('stg_transactions') }}
        WHERE transaction_date::DATE = CURRENT_DATE
        GROUP BY account_id
    ) t ON a.account_id = t.account_id
    
    {% if is_incremental() %}
    WHERE CURRENT_DATE > (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM daily_snapshots