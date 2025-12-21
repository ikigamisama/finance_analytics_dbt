{{
    config(
        materialized='table',
        tags=['gold',  'serving', 'dimension']
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_dimension AS (
    SELECT
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
        
        -- Natural Key
        product_id,
        
        -- Product Attributes
        product_name,
        category,
        product_line,
        
        -- Financial Terms
        interest_rate,
        interest_rate_pct,
        min_balance,
        monthly_fee,
        overdraft_limit,
        
        -- Product Characteristics
        product_tier,
        is_premium,
        product_type_desc,
        
        -- Classifications
        fee_category,
        rate_category,
        complexity_score,
        target_segment,
        risk_level,
        revenue_model,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dw_created_at,
        CURRENT_TIMESTAMP AS dw_updated_at
        
    FROM source
)

SELECT * FROM product_dimension;