{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'marketing_campaigns']
    )
}}

WITH campaign_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['mc.campaign_id']) }} AS campaign_key,
        {{ dbt_utils.generate_surrogate_key(['mc.start_date::date']) }} AS start_date_key,
        {{ dbt_utils.generate_surrogate_key(['mc.end_date::date']) }} AS end_date_key,
        
        mc.campaign_id,
        mc.campaign_name,
        mc.campaign_type,
        mc.start_date,
        mc.end_date,
        mc.target_segment,
        mc.product_promoted,
        mc.campaign_status,
        mc.roi_category,
        mc.campaign_duration_days,
        
        -- Financial Metrics
        mc.budget,
        mc.cost_per_acquisition,
        mc.roi,
        
        -- Performance Metrics
        mc.impressions,
        mc.clicks,
        mc.conversions,
        mc.click_through_rate,
        mc.conversion_rate,
        
        -- Calculated Efficiency Metrics
        CASE 
            WHEN mc.budget > 0 
            THEN ROUND((mc.conversions::NUMERIC / mc.budget::NUMERIC * 1000), 2)
            ELSE 0
        END AS conversions_per_1k_budget,
        
        CASE
            WHEN mc.impressions > 0
            THEN ROUND((mc.budget::NUMERIC / mc.impressions * 1000), 2)
            ELSE 0
        END AS cost_per_1k_impressions,
        
        -- Revenue (estimated: conversions * avg product value assumption)
        mc.conversions * mc.cost_per_acquisition * mc.roi AS estimated_revenue,
        
        -- Flags
        CASE WHEN mc.roi > 1 THEN 1 ELSE 0 END AS profitable_flag,
        CASE WHEN mc.roi >= 2 THEN 1 ELSE 0 END AS highly_profitable_flag,
        CASE WHEN mc.click_through_rate >= 2 THEN 1 ELSE 0 END AS high_engagement_flag,
        CASE WHEN mc.conversion_rate >= 5 THEN 1 ELSE 0 END AS high_conversion_flag,
        CASE WHEN mc.campaign_status = 'Active' THEN 1 ELSE 0 END AS active_campaign_flag,
        CASE WHEN mc.campaign_status = 'Completed' THEN 1 ELSE 0 END AS completed_campaign_flag,
        
        -- Counts
        1 AS campaign_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_marketing_campaigns') }} mc
)

SELECT * FROM campaign_facts