{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'fraud_alerts']
    )
}}

WITH fraud_alert_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['fa.alert_id']) }} AS alert_key,
        {{ dbt_utils.generate_surrogate_key(['fa.transaction_id']) }} AS transaction_key,
        {{ dbt_utils.generate_surrogate_key(['fa.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['fa.account_id']) }} AS account_key,
        {{ dbt_utils.generate_surrogate_key(['fa.alert_date::date']) }} AS alert_date_key,
        {{ dbt_utils.generate_surrogate_key(['fa.resolution_date::date']) }} AS resolution_date_key,
        
        fa.alert_id,
        fa.alert_date,
        fa.alert_type,
        fa.alert_severity,
        fa.investigation_status,
        fa.resolution_date,
        fa.assigned_to,
        
        -- Measures
        fa.amount_recovered,
        fa.resolution_days,
        
        -- Flags
        CASE WHEN fa.is_resolved THEN 1 ELSE 0 END AS resolved_flag,
        CASE WHEN fa.is_confirmed_fraud THEN 1 ELSE 0 END AS confirmed_fraud_flag,
        CASE WHEN fa.is_false_positive THEN 1 ELSE 0 END AS false_positive_flag,
        
        -- Counts
        1 AS alert_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_fraud_alerts') }} fa
)

SELECT * FROM fraud_alert_facts