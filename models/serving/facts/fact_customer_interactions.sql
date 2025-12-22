{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'fact', 'serving', 'customer_interactions']
    )
}}

WITH interaction_facts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ci.interaction_id']) }} AS interaction_key,
        {{ dbt_utils.generate_surrogate_key(['ci.customer_id']) }} AS customer_key,
        {{ dbt_utils.generate_surrogate_key(['ci.interaction_date::date']) }} AS interaction_date_key,
        
        ci.interaction_id,
        ci.interaction_date,
        ci.interaction_year,
        ci.interaction_month,
        ci.interaction_type,
        ci.reason,
        ci.duration_category,
        ci.sentiment_category,
        ci.issue_severity,
        ci.agent_id,
        
        -- Measures
        ci.duration_minutes,
        ci.sentiment_score,
        ci.satisfaction_rating,
        
        -- Flags
        CASE WHEN ci.resolved THEN 1 ELSE 0 END AS resolved_flag,
        CASE WHEN ci.escalated THEN 1 ELSE 0 END AS escalated_flag,
        CASE WHEN ci.sentiment_category = 'Positive' THEN 1 ELSE 0 END AS positive_sentiment_flag,
        CASE WHEN ci.sentiment_category = 'Negative' THEN 1 ELSE 0 END AS negative_sentiment_flag,
        
        -- Counts
        1 AS interaction_count,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM {{ ref('stg_customer_interactions') }} ci
)

SELECT * FROM interaction_facts