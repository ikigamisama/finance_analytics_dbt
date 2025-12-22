{{
    config(
        materialized='table',
        schema='silver',
        tags=['silver', 'transform', 'credit_applications']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ingestion_raw_data', 'credit_applications') }}
),

cleaned AS (
    SELECT
        application_id,
        customer_id,
        product_id,
        application_date::timestamp AS application_date,
        
        -- Application Details
        requested_amount,
        requested_term_months,
        credit_score_at_application,
        annual_income,
        debt_to_income_ratio,
        employment_length_years,
        
        -- DTI Categories
        CASE
            WHEN debt_to_income_ratio < 0.20 THEN 'Excellent'
            WHEN debt_to_income_ratio < 0.36 THEN 'Good'
            WHEN debt_to_income_ratio < 0.43 THEN 'Fair'
            ELSE 'Poor'
        END AS dti_category,
        
        -- Decision
        decision,
        decision_date::timestamp AS decision_date,
        approved_amount,
        approved_rate,
        application_channel,
        
        -- Scores
        approval_probability_score,
        risk_grade,
        
        -- Processing Time
        EXTRACT(DAY FROM (decision_date::timestamp - application_date::timestamp)) AS processing_days,
        
        -- Approval Flag
        CASE WHEN decision = 'Approved' THEN TRUE ELSE FALSE END AS is_approved,
        
        -- Amount Difference
        CASE 
            WHEN approved_amount IS NOT NULL 
            THEN approved_amount - requested_amount 
            ELSE NULL 
        END AS amount_difference,
        
        CURRENT_TIMESTAMP AS updated_at
        
    FROM source
    WHERE application_id IS NOT NULL
)

SELECT * FROM cleaned
