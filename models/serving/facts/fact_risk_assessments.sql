{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'risk_assessments']
    )
}}

WITH risk_assessment_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ra.assessment_id']) }} AS assessment_key,
        {{ dbt_utils.generate_surrogate_key(['ra.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['ra.assessment_date::date']) }} AS assessment_date_key,
        {{ dbt_utils.generate_surrogate_key(['ra.next_review_date::date']) }} AS next_review_date_key,
        
        ra.assessment_id,
        ra.assessment_date,
        ra.assessment_type,
        ra.next_review_date,
        ra.risk_rating,
        ra.credit_risk,
        ra.fraud_risk,
        ra.aml_risk,
        ra.kyc_status,
        ra.kyc_last_updated,
        ra.assessor_id,
        
        -- Risk Metrics
        ra.risk_score,
        ra.transaction_volume_last_90d,
        ra.num_accounts,
        ra.years_as_customer,
        
        -- Composite Risk Score (weighted average)
        CASE
            WHEN ra.credit_risk = 'High' THEN 3
            WHEN ra.credit_risk = 'Medium' THEN 2
            WHEN ra.credit_risk = 'Low' THEN 1
            ELSE 0
        END AS credit_risk_score,
        
        CASE
            WHEN ra.fraud_risk = 'High' THEN 3
            WHEN ra.fraud_risk = 'Medium' THEN 2
            WHEN ra.fraud_risk = 'Low' THEN 1
            ELSE 0
        END AS fraud_risk_score,
        
        CASE
            WHEN ra.aml_risk = 'High' THEN 3
            WHEN ra.aml_risk = 'Medium' THEN 2
            WHEN ra.aml_risk = 'Low' THEN 1
            ELSE 0
        END AS aml_risk_score,
        
        -- Days Until Next Review
        CASE
            WHEN ra.next_review_date IS NOT NULL
            THEN ra.next_review_date::DATE - CURRENT_DATE
            ELSE NULL
        END AS days_until_review,
        
        -- KYC Freshness
        CASE
            WHEN ra.kyc_last_updated IS NOT NULL
            THEN CURRENT_DATE - ra.kyc_last_updated::DATE
            ELSE NULL
        END AS kyc_age_days,
        
        -- Flags
        CASE WHEN ra.pep_flag THEN 1 ELSE 0 END AS pep_flag,
        CASE WHEN ra.sanctions_flag THEN 1 ELSE 0 END AS sanctions_flag,
        CASE WHEN ra.adverse_media_flag THEN 1 ELSE 0 END AS adverse_media_flag,
        CASE WHEN ra.high_value_customer THEN 1 ELSE 0 END AS high_value_flag,
        CASE WHEN ra.requires_enhanced_due_diligence THEN 1 ELSE 0 END AS edd_required_flag,
        CASE WHEN ra.employment_verified THEN 1 ELSE 0 END AS employment_verified_flag,
        CASE WHEN ra.income_verified THEN 1 ELSE 0 END AS income_verified_flag,
        CASE WHEN ra.address_verified THEN 1 ELSE 0 END AS address_verified_flag,
        CASE WHEN ra.kyc_status = 'Compliant' THEN 1 ELSE 0 END AS kyc_compliant_flag,
        
        -- Overall Risk Flag (any high-risk indicator)
        CASE
            WHEN ra.risk_rating = 'High' 
                OR ra.credit_risk = 'High'
                OR ra.fraud_risk = 'High'
                OR ra.aml_risk = 'High'
                OR ra.pep_flag
                OR ra.sanctions_flag
            THEN 1
            ELSE 0
        END AS high_risk_customer_flag,
        
        -- Counts
        1 AS assessment_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_risk_assessments') }} ra
)

SELECT * FROM risk_assessment_facts