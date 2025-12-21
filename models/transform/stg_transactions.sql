{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        tags=['silver', 'ingestion', 'transactions']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ingestion_raw_data', 'transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- Primary Keys
        transaction_id,
        account_id,
        customer_id,
        merchant_id,
        
        -- Transaction Details
        transaction_date,
        EXTRACT(YEAR FROM transaction_date) AS transaction_year,
        EXTRACT(MONTH FROM transaction_date) AS transaction_month,
        EXTRACT(DAY FROM transaction_date) AS transaction_day,
        EXTRACT(HOUR FROM transaction_date) AS transaction_hour,
        EXTRACT(DOW FROM transaction_date) AS day_of_week,
        
        -- Date Flags
        CASE WHEN EXTRACT(DOW FROM transaction_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(HOUR FROM transaction_date) BETWEEN 22 AND 6 THEN TRUE ELSE FALSE END AS is_late_night,
        
        transaction_type,
        amount,
        ABS(amount) AS amount_abs,
        CASE WHEN amount < 0 THEN 'Debit' ELSE 'Credit' END AS transaction_direction,
        currency,
        channel,
        
        -- Merchant Information
        merchant_category,
        mcc_code,
        TRIM(description) AS description,
        
        -- Fraud Detection
        is_fraud,
        fraud_score,
        
        -- Enhanced Fraud Indicators
        CASE
            WHEN fraud_score >= {{ var('fraud_threshold') }} THEN 'High Risk'
            WHEN fraud_score >= 0.4 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS fraud_risk_category,
        
        CASE
            WHEN ABS(amount) > {{ var('high_risk_amount') }} THEN TRUE
            ELSE FALSE
        END AS is_high_value,
        
        -- Location
        location_city,
        location_state,
        location_country,
        latitude,
        longitude,
        is_international,
        
        -- Device & Security
        device_id,
        ip_address,
        authorization_code,
        card_last_four,
        
        -- Behavioral Features
        is_recurring,
        hour_of_day,
        is_weekend AS weekend_flag,
        distance_from_home_km,
        merchant_risk_score,
        velocity_24h,
        amount_deviation_score,
        processing_time_ms,
        decline_reason,
        
        -- Status Flag
        CASE
            WHEN decline_reason IS NOT NULL THEN 'Declined'
            WHEN is_fraud THEN 'Fraud'
            ELSE 'Approved'
        END AS transaction_status,
        
        -- Metadata
        CURRENT_TIMESTAMP AS updated_at
        
    FROM source
    WHERE transaction_id IS NOT NULL
      AND account_id IS NOT NULL
)

SELECT * FROM cleaned