{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'customer_segments']
    )
}}

WITH segment_history_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['csh.segment_history_id']) }} AS segment_history_key,
        {{ dbt_utils.generate_surrogate_key(['csh.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['csh.effective_date::date']) }} AS effective_date_key,
        {{ dbt_utils.generate_surrogate_key(['csh.end_date::date']) }} AS end_date_key,
        
        csh.segment_history_id,
        csh.effective_date,
        csh.end_date,
        csh.is_current,
        
        -- Segment Information
        csh.customer_segment,
        csh.previous_segment,
        csh.loyalty_tier,
        csh.previous_tier,
        csh.risk_segment,
        csh.previous_risk,
        csh.change_type,
        csh.change_reason,
        csh.triggered_by,
        
        -- Customer Metrics at Change
        csh.total_accounts,
        csh.total_balance,
        csh.avg_monthly_transactions,
        csh.products_held,
        csh.customer_lifetime_value,
        csh.tenure_days,
        csh.credit_score,
        csh.annual_income,
        csh.last_interaction_days,
        csh.digital_engagement_score,
        csh.branch_visits_last_90d,
        csh.online_logins_last_90d,
        
        -- Segment Change Analysis
        CASE
            WHEN csh.customer_segment != csh.previous_segment THEN 1
            ELSE 0
        END AS segment_changed_flag,
        
        CASE
            WHEN csh.loyalty_tier != csh.previous_tier THEN 1
            ELSE 0
        END AS tier_changed_flag,
        
        CASE
            WHEN csh.risk_segment != csh.previous_risk THEN 1
            ELSE 0
        END AS risk_changed_flag,
        
        -- Upgrade/Downgrade Detection
        CASE
            WHEN csh.loyalty_tier > csh.previous_tier THEN 'Upgrade'
            WHEN csh.loyalty_tier < csh.previous_tier THEN 'Downgrade'
            ELSE 'No Change'
        END AS tier_movement,
        
        -- Flags
        CASE WHEN csh.eligible_for_premium THEN 1 ELSE 0 END AS premium_eligible_flag,
        CASE 
            WHEN csh.churn_risk IN ('High', 'Medium') THEN 1
            ELSE 0
        END AS churn_risk_flag,
        CASE WHEN csh.cross_sell_opportunity THEN 1 ELSE 0 END AS cross_sell_opportunity_flag,
        
        -- Days in Segment
        CASE 
            WHEN csh.is_current THEN CURRENT_DATE - csh.effective_date::DATE
            ELSE csh.end_date::DATE - csh.effective_date::DATE
        END AS days_in_segment,
        
        -- Counts
        1 AS segment_change_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_customer_segments_history') }} csh
)

SELECT * FROM segment_history_facts