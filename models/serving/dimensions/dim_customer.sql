{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'serving', 'scd2']
    )
}}

WITH customer_current AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} AS customer_key,
        
        -- Natural Key
        customer_id,
        
        -- Personal Information
        first_name,
        last_name,
        first_name || ' ' || last_name AS full_name,
        email,
        phone_clean AS phone,
        date_of_birth,
        age,
        
        -- Age Groups
        CASE
            WHEN age < 25 THEN '18-24'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            WHEN age < 55 THEN '45-54'
            WHEN age < 65 THEN '55-64'
            ELSE '65+'
        END AS age_group,
        
        -- Location
        city,
        state,
        zip_code,
        country,
        
        -- Demographics
        employment_status,
        education_level,
        marital_status,
        number_of_dependents,
        home_ownership,
        
        -- Financial Profile
        credit_score,
        credit_score_band,
        annual_income,
        income_bracket,
        
        -- Segmentation
        customer_segment,
        life_stage,
        risk_segment,
        loyalty_tier,
        
        -- Behavioral
        preferred_channel,
        marketing_opt_in,
        acquisition_channel,
        tenure_months,
        
        -- Metrics
        customer_lifetime_value,
        churn_risk_score,
        churn_risk_category,
        
        -- Status
        is_active,
        signup_date,
        last_login_date,
        
        -- SCD Type 2 Columns
        updated_at AS effective_date,
        '9999-12-31'::DATE AS end_date,
        TRUE AS is_current,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dw_created_at,
        CURRENT_TIMESTAMP AS dw_updated_at
        
    FROM {{ ref('stg_customers') }}
)

SELECT * FROM customer_current