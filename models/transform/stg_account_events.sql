{{
    config(
        materialized='table',
        tags=['silver', 'ingestion', 'account_events']
    )
}}


WITH source AS (
    SELECT * FROM {{ source('ingestion_raw_data', 'account_events') }}
),

cleaned AS (
    SELECT
        -- Primary Keys
        event_id,
        account_id,
        customer_id,
        product_id,
        
        -- Event Details
        event_date,
        TRIM(event_type) AS event_type,
        event_category,
        
        -- Change Values
        old_value,
        new_value,
        
        -- Event Metadata
        triggered_by,
        channel,
        TRIM(processed_by) AS processed_by,
        TRIM(notes) AS notes,
        
        -- Flags
        is_reversible,
        requires_approval,
        approval_status,
        
        -- Metadata
        created_at,
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM source
    WHERE event_id IS NOT NULL
      AND account_id IS NOT NULL
      AND customer_id IS NOT NULL
)

SELECT * FROM cleaned