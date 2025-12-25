{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'diagnostic', 'ingestion', 'marketing']
    )
}}

SELECT
    mc.campaign_type,
    mc.target_segment,
    dc.channel_group,
    mc.roi_category,
    mc.campaign_status,
    
    COUNT(*) AS campaign_count,
    
    -- Performance metrics
    ROUND(AVG(mc.roi)::numeric, 2) AS avg_roi,
    ROUND(AVG(mc.click_through_rate)::numeric, 2) AS avg_ctr,
    ROUND(AVG(mc.conversion_rate)::numeric, 2) AS avg_conversion_rate,
    ROUND(AVG(mc.cost_per_acquisition)::numeric, 2) AS avg_cpa,
    
    -- Efficiency metrics
    ROUND(AVG(mc.conversions_per_1k_budget)::numeric, 2) AS avg_conversions_per_1k,
    ROUND(AVG(mc.cost_per_1k_impressions)::numeric, 2) AS avg_cpm,
    
    -- Volume metrics
    ROUND(AVG(mc.impressions)::numeric, 0) AS avg_impressions,
    ROUND(AVG(mc.clicks)::numeric, 0) AS avg_clicks,
    ROUND(AVG(mc.conversions)::numeric, 0) AS avg_conversions,
    ROUND(AVG(mc.budget)::numeric, 2) AS avg_budget,
    ROUND(AVG(mc.campaign_duration_days)::numeric, 1) AS avg_duration_days,
    
    -- Success indicators
    SUM(mc.profitable_flag) AS profitable_campaigns,
    ROUND((SUM(mc.profitable_flag) * 100.0 / COUNT(*))::numeric, 2) AS profitable_rate_pct,
    
    CURRENT_TIMESTAMP AS last_updated
    
FROM {{ ref('fact_marketing_campaigns') }} mc
INNER JOIN {{ ref('dim_campaign') }} dc ON mc.campaign_key = dc.campaign_key
GROUP BY mc.campaign_type, mc.target_segment, dc.channel_group, mc.roi_category, mc.campaign_status
ORDER BY avg_roi DESC