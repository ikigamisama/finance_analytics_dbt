{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        tags=['gold', 'fact', 'transactions']
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customer') }}
    WHERE is_current = TRUE
),

products AS (
    SELECT product_key, product_id FROM {{ ref('dim_product') }}
),

merchants AS (
    SELECT merchant_key, merchant_id FROM {{ ref('dim_merchant') }}
),

accounts AS (
    SELECT account_id, product_id, customer_id FROM {{ ref('stg_accounts') }}
),

fact_table AS (
    SELECT
        -- Fact Primary Key
        t.transaction_id,
        
        -- Foreign Keys (Dimension References)
        c.customer_key,
        p.product_key,
        m.merchant_key,
        TO_CHAR(t.transaction_date, 'YYYYMMDD')::INTEGER AS date_key,
        
        -- Degenerate Dimensions (Transaction-specific attributes)
        t.account_id,
        t.transaction_type,
        t.channel,
        t.transaction_status,
        t.merchant_category,
        t.mcc_code,
        t.authorization_code,
        
        -- Date/Time Attributes
        t.transaction_date,
        t.transaction_year,
        t.transaction_month,
        t.transaction_day,
        t.transaction_hour,
        t.day_of_week,
        t.is_weekend,
        t.is_late_night,
        
        -- Measures (Additive)
        t.amount AS transaction_amount,
        t.amount_abs AS transaction_amount_abs,
        
        -- Binary Flags (Semi-Additive)
        t.is_fraud::INTEGER AS fraud_flag,
        t.is_high_value::INTEGER AS high_value_flag,
        t.is_international::INTEGER AS international_flag,
        t.is_recurring::INTEGER AS recurring_flag,
        CASE WHEN t.transaction_status = 'Approved' THEN 1 ELSE 0 END AS approved_flag,
        CASE WHEN t.transaction_status = 'Declined' THEN 1 ELSE 0 END AS declined_flag,
        
        -- Fraud & Risk Metrics (Non-Additive)
        t.fraud_score,
        t.fraud_risk_category,
        t.merchant_risk_score,
        t.velocity_24h,
        t.amount_deviation_score,
        
        -- Location Attributes
        t.location_city,
        t.location_state,
        t.location_country,
        t.distance_from_home_km,
        
        -- Performance Metrics
        t.processing_time_ms,
        
        -- Derived Measures
        CASE 
            WHEN t.amount < 0 THEN t.amount_abs 
            ELSE 0 
        END AS debit_amount,
        
        CASE 
            WHEN t.amount > 0 THEN t.amount_abs 
            ELSE 0 
        END AS credit_amount,
        
        -- Transaction Count (for aggregation)
        1 AS transaction_count,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dw_created_at
        
    FROM transactions t
    INNER JOIN accounts a ON t.account_id = a.account_id
    LEFT JOIN customers c ON a.customer_id = c.customer_id
    LEFT JOIN products p ON a.product_id = p.product_id
    LEFT JOIN merchants m ON t.merchant_id = m.merchant_id
)

SELECT * FROM fact_table