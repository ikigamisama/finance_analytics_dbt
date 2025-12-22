{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'regulatory']
    )
}}


WITH regulatory_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['rr.report_id']) }} AS report_key,
        {{ dbt_utils.generate_surrogate_key(['rr.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['rr.account_id']) }} AS account_key,
        {{ dbt_utils.generate_surrogate_key(['rr.transaction_id']) }} AS transaction_key,
        {{ dbt_utils.generate_surrogate_key(['rr.filing_date::date']) }} AS filing_date_key,
        {{ dbt_utils.generate_surrogate_key(['rr.due_date::date']) }} AS due_date_key,
        
        rr.report_id,
        rr.report_type_code,
        rr.report_type_name,
        rr.report_frequency,
        rr.regulator,
        rr.report_period_start,
        rr.report_period_end,
        rr.filing_date,
        rr.due_date,
        rr.actual_filing_date,
        rr.filing_status,
        rr.filing_method,
        rr.confirmation_number,
        rr.risk_level,
        rr.assigned_to,
        rr.reviewed_by,
        rr.approval_date,
        
        -- Measures
        rr.amount_reported,
        rr.penalty_amount,
        
        -- Timeliness Metrics
        CASE 
            WHEN rr.actual_filing_date IS NOT NULL AND rr.due_date IS NOT NULL
            THEN rr.actual_filing_date::DATE - rr.due_date::DATE
            ELSE NULL
        END AS days_from_due_date,
        
        CASE
            WHEN rr.actual_filing_date IS NOT NULL AND rr.filing_date IS NOT NULL
            THEN rr.actual_filing_date::DATE - rr.filing_date::DATE
            ELSE NULL
        END AS processing_days,
        
        -- Flags
        CASE WHEN rr.filing_status = 'Filed' THEN 1 ELSE 0 END AS filed_flag,
        CASE WHEN rr.filing_status = 'Pending' THEN 1 ELSE 0 END AS pending_flag,
        CASE WHEN rr.filing_status = 'Late' THEN 1 ELSE 0 END AS late_flag,
        CASE WHEN rr.requires_follow_up THEN 1 ELSE 0 END AS requires_follow_up_flag,
        CASE WHEN rr.is_amended THEN 1 ELSE 0 END AS amended_flag,
        CASE WHEN rr.penalty_amount > 0 THEN 1 ELSE 0 END AS penalty_assessed_flag,
        CASE 
            WHEN rr.actual_filing_date IS NOT NULL 
                AND rr.due_date IS NOT NULL 
                AND rr.actual_filing_date::DATE > rr.due_date::DATE 
            THEN 1 
            ELSE 0 
        END AS filed_late_flag,
        
        -- Risk Classification
        CASE
            WHEN rr.risk_level = 'High' THEN 3
            WHEN rr.risk_level = 'Medium' THEN 2
            WHEN rr.risk_level = 'Low' THEN 1
            ELSE 0
        END AS risk_score,
        
        -- Counts
        1 AS report_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_regulatory_reports') }} rr
)

SELECT * FROM regulatory_facts