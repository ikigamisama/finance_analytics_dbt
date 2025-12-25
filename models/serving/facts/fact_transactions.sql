{{
    config(
        materialized='incremental',
        schema="gold",
        unique_key='transaction_key',
        tags=['gold', 'fact', 'serving', 'transactions']
    )
}}

WITH transaction_facts AS (
    SELECT
        t.transaction_id,
        
        -- Surrogate Keys (Dimension References)
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} AS transaction_key,
        {{ dbt_utils.generate_surrogate_key(['t.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['t.account_id']) }} AS account_key,
        {{ dbt_utils.generate_surrogate_key(['t.merchant_id']) }} AS merchant_key,
        {{ dbt_utils.generate_surrogate_key(['t.transaction_date::date']) }} AS date_key,
        
        -- Degenerate Dimensions (Transaction Attributes)
        t.transaction_date,
        t.transaction_year,
        t.transaction_month,
        t.transaction_day,
        t.transaction_hour,
        t.day_of_week,
        t.is_weekend,
        t.is_late_night,
        t.transaction_type,
        t.transaction_direction,
        t.currency,
        t.channel,
        t.merchant_category,
        t.mcc_code,
        t.description,
        t.location_city,
        t.location_state,
        t.location_country,
        t.is_international,
        t.device_id,
        t.authorization_code,
        t.card_last_four,
        t.is_recurring,
        t.transaction_status,
        t.decline_reason,
        t.fraud_risk_category,
        
        -- Measures (Facts)
        t.amount AS transaction_amount,
        t.amount_abs AS transaction_amount_abs,
        t.fraud_score,
        t.distance_from_home_km,
        t.merchant_risk_score,
        t.velocity_24h,
        t.amount_deviation_score,
        t.processing_time_ms,
        
        -- Flags (Boolean Facts)
        t.is_fraud AS is_fraud_flag,
        t.is_high_value AS is_high_value_flag,
        CASE WHEN t.fraud_risk_category = 'High Risk' THEN 1 ELSE 0 END AS is_high_risk_flag,
        CASE WHEN t.decline_reason IS NOT NULL THEN 1 ELSE 0 END AS is_declined_flag,
        
        -- Calculated Measures
        CASE WHEN t.is_fraud THEN t.amount_abs ELSE 0 END AS fraud_amount,
        CASE WHEN t.decline_reason IS NOT NULL THEN 1 ELSE 0 END AS declined_count,
        1 AS transaction_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_transactions') }} t
    {% if is_incremental() %}
    WHERE t.transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM transaction_facts