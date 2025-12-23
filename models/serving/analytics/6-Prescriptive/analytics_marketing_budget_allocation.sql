{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'prescriptive', 'serving', 'marketing']
    )
}}

WITH campaign_performance AS (
    SELECT
        mc.campaign_type,
        dc.channel_group,
        mc.target_segment,
        
        COUNT(*) AS campaign_count,
        SUM(mc.budget) AS total_budget,
        SUM(mc.conversions) AS total_conversions,
        AVG(mc.roi) AS avg_roi,
        AVG(mc.cost_per_acquisition) AS avg_cpa,
        AVG(mc.conversion_rate) AS avg_conversion_rate,
        
        -- Calculate efficiency score
        AVG(mc.roi) * AVG(mc.conversion_rate) / NULLIF(AVG(mc.cost_per_acquisition), 0) AS efficiency_score
        
    FROM {{ ref('fact_marketing_campaigns') }} mc
    INNER JOIN {{ ref('dim_campaign') }} dc ON mc.campaign_key = dc.campaign_key
    WHERE mc.campaign_status = 'Completed'
      AND mc.end_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY mc.campaign_type, dc.channel_group, mc.target_segment
)

SELECT
    campaign_type,
    channel_group,
    target_segment,
    campaign_count,
    ROUND(total_budget, 2) AS historical_budget,
    ROUND(avg_roi, 2) AS avg_roi,
    ROUND(avg_cpa, 2) AS avg_cpa,
    ROUND(avg_conversion_rate, 2) AS avg_conversion_rate_pct,
    ROUND(efficiency_score, 4) AS efficiency_score,
    
    -- Recommended budget allocation (proportional to efficiency)
    ROUND(
        1000000 *  -- Total budget to allocate
        efficiency_score / SUM(efficiency_score) OVER ()
    , 2) AS recommended_budget,
    
    ROUND(
        recommended_budget - (total_budget / campaign_count)
    , 2) AS budget_change,
    
    -- Expected outcomes
    ROUND(recommended_budget * avg_roi, 2) AS expected_revenue,
    ROUND(recommended_budget / NULLIF(avg_cpa, 0), 0) AS expected_conversions,
    
    -- Priority ranking
    ROW_NUMBER() OVER (ORDER BY efficiency_score DESC) AS priority_rank,
    
    -- Recommendation
    CASE
        WHEN efficiency_score >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY efficiency_score) OVER ()
            THEN 'Increase Budget'
        WHEN efficiency_score <= PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY efficiency_score) OVER ()
            THEN 'Decrease Budget'
        ELSE 'Maintain Budget'
    END AS budget_recommendation,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM campaign_performance
ORDER BY efficiency_score DESC