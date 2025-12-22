{{
    config(
        materialized='table',
        tags=['silver', 'ingestion', 'marketing']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('ingestion_raw_data', 'marketing_campaigns') }}
),

cleaned AS (
    SELECT
        campaign_id,
        campaign_name,
        campaign_type,
        start_date::timestamp as start_date,
        end_date::timestamp as end_date,
        target_segment,
        budget,
        impressions,
        clicks,
        conversions,
        cost_per_acquisition,
        roi,
        product_promoted,
        
        -- Campaign Duration
        EXTRACT(DAY FROM (end_date - start_date)) AS campaign_duration_days,
        
        -- Performance Metrics
        CASE 
            WHEN impressions > 0 
            THEN ROUND((clicks::NUMERIC / impressions * 100), 2) 
            ELSE 0 
        END AS click_through_rate,
        
        CASE 
            WHEN clicks > 0 
            THEN ROUND((conversions::NUMERIC / clicks * 100), 2) 
            ELSE 0 
        END AS conversion_rate,
        
        -- ROI Category
        CASE
            WHEN roi >= 2.0 THEN 'Excellent'
            WHEN roi >= 1.0 THEN 'Good'
            WHEN roi >= 0 THEN 'Break Even'
            ELSE 'Loss'
        END AS roi_category,
        
        -- Campaign Status
        CASE
            WHEN CURRENT_DATE < start_date THEN 'Scheduled'
            WHEN CURRENT_DATE BETWEEN start_date AND end_date THEN 'Active'
            ELSE 'Completed'
        END AS campaign_status,
        
        CURRENT_TIMESTAMP AS updated_at
        
    FROM source
    WHERE campaign_id IS NOT NULL
)

SELECT * FROM cleaned