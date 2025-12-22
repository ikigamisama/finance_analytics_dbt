{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'loan_payments']
    )
}}

WITH payment_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['lp.payment_id']) }} AS payment_key,
        {{ dbt_utils.generate_surrogate_key(['lp.account_id']) }} AS account_key,
        {{ dbt_utils.generate_surrogate_key(['lp.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['lp.scheduled_date::date']) }} AS scheduled_date_key,
        {{ dbt_utils.generate_surrogate_key(['lp.actual_date::date']) }} AS actual_date_key,
        
        lp.payment_id,
        lp.scheduled_date,
        lp.actual_date,
        lp.payment_status,
        lp.payment_method,
        lp.payment_completeness,
        lp.delinquency_bucket,
        
        -- Measures
        lp.scheduled_amount,
        lp.actual_amount,
        lp.amount_difference,
        lp.late_fee,
        lp.outstanding_balance,
        lp.days_late,
        
        -- Flags
        CASE WHEN lp.is_late THEN 1 ELSE 0 END AS late_payment_flag,
        CASE WHEN lp.payment_status = 'Missed' THEN 1 ELSE 0 END AS missed_payment_flag,
        CASE WHEN lp.payment_completeness = 'Full' THEN 1 ELSE 0 END AS full_payment_flag,
        
        -- Counts
        1 AS payment_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_loan_payments') }} lp
)

SELECT * FROM payment_facts