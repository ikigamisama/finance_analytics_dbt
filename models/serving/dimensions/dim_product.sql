{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'dimension', 'serving', 'product']
    )
}}

WITH product_enhanced AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
        product_id AS product_natural_key,
        product_name,
        category,
        product_line,
        interest_rate,
        interest_rate_pct,
        min_balance,
        monthly_fee,
        overdraft_limit,
        product_tier,
        is_premium,
        product_type_desc,
        fee_category,
        rate_category,
        complexity_score,
        target_segment,
        risk_level,
        revenue_model,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM {{ ref('stg_products') }}
)

SELECT * FROM product_enhanced