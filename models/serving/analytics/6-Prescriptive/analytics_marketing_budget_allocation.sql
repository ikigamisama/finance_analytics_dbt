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
),
budget_calculated AS (
    SELECT
        *,
        ROUND(
            (1000000 * efficiency_score / SUM(efficiency_score) OVER ())::numeric,
        2) AS recommended_budget
    FROM campaign_performance
),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY efficiency_score) AS p75,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY efficiency_score) AS p25
    FROM budget_calculated
)
SELECT
    bc.*,
    ROUND((recommended_budget - (total_budget / campaign_count))::numeric, 2) AS budget_change,
    ROUND((recommended_budget * avg_roi)::numeric, 2) AS expected_revenue,
    ROUND((recommended_budget / NULLIF(avg_cpa, 0))::numeric, 0) AS expected_conversions,
    ROW_NUMBER() OVER (ORDER BY efficiency_score DESC) AS priority_rank,
    
    CASE
        WHEN bc.efficiency_score >= p.p75 THEN 'Increase Budget'
        WHEN bc.efficiency_score <= p.p25 THEN 'Decrease Budget'
        ELSE 'Maintain Budget'
    END AS budget_recommendation,
    
    CURRENT_TIMESTAMP AS generated_at
FROM budget_calculated bc
CROSS JOIN percentiles p
ORDER BY bc.efficiency_score DESC