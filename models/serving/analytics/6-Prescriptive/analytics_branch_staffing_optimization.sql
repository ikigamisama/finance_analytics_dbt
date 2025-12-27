{{
    config(
        materialized = 'table',
        schema = 'gold',
        tags = ['analytics', 'prescriptive', 'serving', 'operations']
    )
}}

WITH branch_activity AS (
    SELECT
        l.location_natural_key AS branch_id,
        l.location_name AS branch_name,
        l.region,
        
        COUNT(DISTINCT t.transaction_key) AS total_transactions,
        COUNT(DISTINCT CASE WHEN t.channel = 'Branch' THEN t.transaction_key END) AS branch_transactions,
        
        AVG(CASE WHEN d.day_of_week IN (1, 2, 3, 4, 5) THEN 1 ELSE 0 END) AS weekday_weight,
        AVG(CASE WHEN EXTRACT(HOUR FROM t.transaction_date) BETWEEN 9 AND 17 THEN 1 ELSE 0 END) AS business_hours_weight
        
    FROM {{ ref('dim_location') }} l
    LEFT JOIN {{ ref('fact_transactions') }} t 
        ON l.location_natural_key::VARCHAR = t.location_city
    LEFT JOIN {{ ref('dim_date') }} d 
        ON t.date_key = d.date_key
    
    WHERE l.location_type = 'BRANCH'
    GROUP BY l.location_natural_key, l.location_name, l.region
),

total_interactions AS (
    SELECT
        COUNT(*) AS interaction_count
    FROM {{ ref('fact_customer_interactions') }}
    WHERE interaction_type = 'Branch Visit'
      AND interaction_date >= CURRENT_DATE - INTERVAL '90 days'
),

base_calculation AS (
    SELECT
        ba.branch_id,
        ba.branch_name,
        ba.region,
        ba.total_transactions,
        ba.branch_transactions,
        COALESCE(ti.interaction_count, 0) AS service_interactions,
        
        -- Current staffing assumption
        CASE
            WHEN ba.total_transactions >= 10000 THEN 15
            WHEN ba.total_transactions >= 5000 THEN 10
            WHEN ba.total_transactions >= 1000 THEN 6
            ELSE 3
        END AS current_staff_estimate,
        
        -- Recommended staffing
        ROUND(
            3 +
            (ba.branch_transactions / 500.0) +
            (COALESCE(ti.interaction_count, 0) / 200.0 /
             NULLIF(
                 (SELECT COUNT(*) 
                  FROM {{ ref('dim_location') }} 
                  WHERE location_type = 'BRANCH'),
             0))
        , 0) AS recommended_staff,
        
        CURRENT_TIMESTAMP AS generated_at
        
    FROM branch_activity ba
    CROSS JOIN total_interactions ti
),

final AS (
    SELECT
        *,
        recommended_staff - current_staff_estimate AS staff_change,
        ROUND((recommended_staff - current_staff_estimate) * 50000, 2) AS annual_cost_impact,
        ROUND(total_transactions * 1.0 / NULLIF(current_staff_estimate, 0), 0) 
            AS current_transactions_per_staff,
        ROUND(total_transactions * 1.0 / NULLIF(recommended_staff, 0), 0) 
            AS optimal_transactions_per_staff,
        CASE
            WHEN recommended_staff > current_staff_estimate THEN 'Hire Additional Staff'
            WHEN recommended_staff < current_staff_estimate THEN 'Optimize Staffing'
            ELSE 'Maintain Current Staffing'
        END AS staffing_recommendation
    FROM base_calculation
)

SELECT *
FROM final
ORDER BY ABS(staff_change) DESC