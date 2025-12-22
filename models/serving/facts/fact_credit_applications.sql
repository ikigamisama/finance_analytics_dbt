{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'credit_applications']
    )
}}

WITH application_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ca.application_id']) }} AS application_key,
        {{ dbt_utils.generate_surrogate_key(['ca.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['ca.product_id']) }} AS product_key,
        {{ dbt_utils.generate_surrogate_key(['ca.application_date::date']) }} AS application_date_key,
        {{ dbt_utils.generate_surrogate_key(['ca.decision_date::date']) }} AS decision_date_key,
        
        ca.application_id,
        ca.application_date,
        ca.decision_date,
        ca.decision,
        ca.application_channel,
        ca.risk_grade,
        ca.dti_category,
        
        -- Measures
        ca.requested_amount,
        ca.requested_term_months,
        ca.credit_score_at_application,
        ca.annual_income,
        ca.debt_to_income_ratio,
        ca.employment_length_years,
        ca.approved_amount,
        ca.approved_rate,
        ca.approval_probability_score,
        ca.processing_days,
        ca.amount_difference,
        
        -- Flags
        CASE WHEN ca.is_approved THEN 1 ELSE 0 END AS approved_flag,
        CASE WHEN ca.decision = 'Denied' THEN 1 ELSE 0 END AS denied_flag,
        
        -- Counts
        1 AS application_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_credit_applications') }} ca
)

SELECT * FROM application_facts