{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'dimension', 'serving', 'account']
    )
}}

WITH account_enhanced AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} AS account_key,
        account_id AS account_natural_key,
        customer_id,
        product_id,
        account_number,
        account_status,
        open_date,
        close_date,
        account_age_months,
        is_active,
        is_closed,
        is_dormant,
        current_balance,
        available_balance,
        credit_limit,
        credit_utilization_pct,
        balance_category,
        currency,
        interest_rate,
        minimum_payment,
        payment_due_date,
        last_statement_date,
        autopay_enabled,
        overdraft_protection,
        primary_account,
        is_past_due,
        is_near_limit,
        CURRENT_TIMESTAMP AS effective_date,
        '9999-12-31'::TIMESTAMP AS expiration_date,
        TRUE AS is_current,
        CURRENT_TIMESTAMP AS dbt_updated_at
    FROM {{ ref('stg_accounts') }}
)

SELECT * FROM account_enhanced