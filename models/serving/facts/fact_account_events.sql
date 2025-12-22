{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'account_events']
    )
}}

WITH account_event_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ae.event_id']) }} AS event_key,
        {{ dbt_utils.generate_surrogate_key(['ae.account_id']) }} AS account_key,
        {{ dbt_utils.generate_surrogate_key(['ae.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['ae.product_id']) }} AS product_key,
        {{ dbt_utils.generate_surrogate_key(['ae.event_date::date']) }} AS event_date_key,
        
        ae.event_id,
        ae.event_date,
        ae.event_type,
        ae.event_category,
        ae.triggered_by,
        ae.channel,
        ae.processed_by,
        ae.approval_status,
        
        -- Event Values
        ae.old_value,
        ae.new_value,
        
        -- Attempt to parse numeric changes
        CASE 
            WHEN ae.old_value ~ '^[0-9.]+ AND ae.new_value ~ '^[0-9.]+
            THEN ae.new_value::NUMERIC - ae.old_value::NUMERIC
            ELSE NULL
        END AS value_change,
        
        -- Flags
        CASE WHEN ae.is_reversible THEN 1 ELSE 0 END AS reversible_flag,
        CASE WHEN ae.requires_approval THEN 1 ELSE 0 END AS requires_approval_flag,
        CASE WHEN ae.approval_status = 'Approved' THEN 1 ELSE 0 END AS approved_flag,
        CASE WHEN ae.approval_status = 'Pending' THEN 1 ELSE 0 END AS pending_flag,
        CASE WHEN ae.approval_status = 'Rejected' THEN 1 ELSE 0 END AS rejected_flag,
        
        -- Event Type Categories
        CASE
            WHEN ae.event_type IN ('Account Opened', 'Account Closed', 'Account Reactivated') 
                THEN 'Lifecycle'
            WHEN ae.event_type IN ('Balance Update', 'Credit Limit Change', 'Interest Rate Change')
                THEN 'Financial'
            WHEN ae.event_type IN ('Status Change', 'Ownership Transfer')
                THEN 'Administrative'
            ELSE 'Other'
        END AS event_type_category,
        
        -- Counts
        1 AS event_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_account_events') }} ae
)

SELECT * FROM account_event_facts