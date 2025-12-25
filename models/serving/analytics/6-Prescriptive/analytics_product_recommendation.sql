{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'prescriptive', 'serving', 'products']
    )
}}

WITH customer_products AS (
    SELECT
        c.customer_key,
        c.customer_natural_key,
        c.customer_segment,
        c.annual_income,
        c.credit_score_band,
        c.tenure_months,
        STRING_AGG(DISTINCT p.category, ', ') AS current_products,
        COUNT(DISTINCT a.account_key) AS product_count,
        SUM(a.current_balance) AS total_balance
    FROM {{ ref('dim_customer') }} c
    INNER JOIN {{ ref('dim_account') }} a 
        ON c.customer_natural_key = a.customer_id 
        AND a.is_current = TRUE AND a.is_active = TRUE
    INNER JOIN {{ ref('dim_product') }} p 
        ON a.product_id = p.product_natural_key
    WHERE c.is_current = TRUE AND c.is_active = TRUE
    GROUP BY c.customer_key, c.customer_natural_key, c.customer_segment, 
             c.annual_income, c.credit_score_band, c.tenure_months
),

product_recommendations_base AS (
    SELECT
        cp.customer_key,
        cp.customer_natural_key,
        cp.customer_segment,
        cp.current_products,
        cp.product_count,
        cp.tenure_months,
        cp.total_balance,
        cp.credit_score_band,
        cp.annual_income,

        CASE
            WHEN cp.current_products NOT LIKE '%Credit%' 
                AND cp.credit_score_band IN ('Good', 'Very Good', 'Excellent')
                AND cp.tenure_months >= 6
            THEN 'Credit Card'

            WHEN cp.current_products NOT LIKE '%Investment%'
                AND cp.annual_income >= 75000
                AND cp.total_balance >= 10000
            THEN 'Investment Account'

            WHEN cp.product_count = 1 
                AND cp.current_products LIKE '%Deposit%'
                AND cp.tenure_months >= 3
            THEN 'Savings Account'

            WHEN cp.current_products LIKE '%Deposit%'
                AND cp.current_products NOT LIKE '%Credit%'
                AND cp.credit_score_band NOT IN ('Poor', 'Fair')
            THEN 'Credit Card'

            WHEN cp.total_balance >= 50000
                AND cp.customer_segment != 'Premium'
            THEN 'Premium Checking'

            ELSE 'Personal Loan'
        END AS recommended_product

    FROM customer_products cp
),
product_recommendations AS (
    SELECT
        b.*,

        ROUND(
            CASE b.recommended_product
                WHEN 'Credit Card' THEN 75
                WHEN 'Investment Account' THEN 85
                WHEN 'Savings Account' THEN 70
                WHEN 'Premium Checking' THEN 80
                ELSE 60
            END *
            CASE
                WHEN b.tenure_months >= 24 THEN 1.2
                WHEN b.tenure_months >= 12 THEN 1.1
                WHEN b.tenure_months >= 6 THEN 1.0
                ELSE 0.8
            END *
            CASE b.customer_segment
                WHEN 'Premium' THEN 1.3
                WHEN 'Affluent' THEN 1.2
                WHEN 'Mass Market' THEN 1.0
                ELSE 0.9
            END
        , 0) AS recommendation_score,

        CASE b.recommended_product
            WHEN 'Credit Card' THEN 240
            WHEN 'Investment Account' THEN 500
            WHEN 'Savings Account' THEN 120
            WHEN 'Premium Checking' THEN 360
            WHEN 'Personal Loan' THEN 800
            ELSE 150
        END AS expected_annual_revenue,

        CASE
            WHEN b.product_count = 1 AND b.tenure_months >= 12 THEN 45
            WHEN b.customer_segment = 'Premium' THEN 55
            WHEN b.customer_segment = 'Affluent' THEN 50
            WHEN b.product_count >= 3 THEN 25
            ELSE 35
        END AS propensity_to_accept_pct

    FROM product_recommendations_base b
)

SELECT
    customer_key,
    customer_natural_key,
    customer_segment,
    current_products,
    product_count,
    recommended_product,
    recommendation_score,
    
    -- Expected value
    ROUND(expected_annual_revenue * (propensity_to_accept_pct / 100.0), 2) AS expected_value,
    
    propensity_to_accept_pct,
    expected_annual_revenue,
    
    -- Campaign details
    CASE
        WHEN recommendation_score >= 80 THEN 'Personal Banker Call'
        WHEN recommendation_score >= 60 THEN 'Targeted Email Campaign'
        ELSE 'In-App Banner'
    END AS recommended_campaign_channel,
    
    CASE
        WHEN recommendation_score >= 80 THEN 'High Priority'
        WHEN recommendation_score >= 60 THEN 'Medium Priority'
        ELSE 'Low Priority'
    END AS campaign_priority,
    
    total_balance,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM product_recommendations
WHERE recommendation_score >= 50  -- Minimum threshold
ORDER BY expected_value DESC