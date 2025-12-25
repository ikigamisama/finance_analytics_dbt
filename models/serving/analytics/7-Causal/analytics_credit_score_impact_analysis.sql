{{
    config(
        materialized='table',
        schema="gold",
        tags=['analytics', 'causal', 'serving', 'credit']
    )
}}

WITH credit_score_cohorts AS (
    SELECT
        c.customer_key,
        c.credit_score,
        c.credit_score_band,
        c.annual_income,
        c.customer_lifetime_value,
        c.tenure_months,
        
        -- Outcomes
        COALESCE(t.transaction_count, 0) AS transaction_count,
        COALESCE(t.total_volume, 0) AS total_transaction_volume,
        COALESCE(a.account_count, 0) AS account_count,
        COALESCE(a.total_balance, 0) AS total_balance,
        COALESCE(ca.approval_rate, 0) AS credit_approval_rate,
        COALESCE(lp.default_flag, 0) AS has_defaulted
        
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN (
        SELECT
            customer_key,
            COUNT(*) AS transaction_count,
            SUM(transaction_amount_abs) AS total_volume
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '365 days'
        GROUP BY customer_key
    ) t ON c.customer_key = t.customer_key
    LEFT JOIN (
        SELECT
            customer_id,
            COUNT(*) AS account_count,
            SUM(current_balance) AS total_balance
        FROM {{ ref('dim_account') }}
        WHERE is_current = TRUE
        GROUP BY customer_id
    ) a ON c.customer_natural_key = a.customer_id
    LEFT JOIN (
        SELECT
            customer_key,
            AVG(approved_flag) AS approval_rate
        FROM {{ ref('fact_credit_applications') }}
        GROUP BY customer_key
    ) ca ON c.customer_key = ca.customer_key
    LEFT JOIN (
        SELECT
            customer_key,
            MAX(CASE WHEN missed_payment_flag = 1 THEN 1 ELSE 0 END) AS default_flag
        FROM {{ ref('fact_loan_payments') }}
        GROUP BY customer_key
    ) lp ON c.customer_key = lp.customer_key
    
    WHERE c.is_current = TRUE
), 
credit_score_band_agg AS (
    SELECT
        credit_score_band,

        COUNT(*) AS customer_count,

        AVG(credit_score) AS avg_credit_score,
        STDDEV(credit_score) AS stddev_credit_score,

        AVG(transaction_count) AS avg_transactions,
        AVG(total_transaction_volume) AS avg_transaction_volume,
        AVG(account_count) AS avg_accounts,
        AVG(total_balance) AS avg_balance,
        AVG(credit_approval_rate) * 100 AS avg_approval_rate_pct,
        AVG(has_defaulted) * 100 AS default_rate_pct,
        AVG(customer_lifetime_value) AS avg_clv

    FROM credit_score_cohorts
    GROUP BY credit_score_band
)


SELECT
    credit_score_band,
    customer_count,

    ROUND(avg_credit_score::numeric, 0) AS avg_credit_score,
    ROUND(stddev_credit_score::numeric, 2) AS stddev_credit_score,

    ROUND(avg_transactions::numeric, 1) AS avg_transactions,
    ROUND(avg_transaction_volume::numeric, 2) AS avg_transaction_volume,
    ROUND(avg_accounts::numeric, 2) AS avg_accounts,
    ROUND(avg_balance::numeric, 2) AS avg_balance,
    ROUND(avg_approval_rate_pct::numeric, 2) AS avg_approval_rate_pct,
    ROUND(default_rate_pct::numeric, 2) AS default_rate_pct,
    ROUND(avg_clv::numeric, 2) AS avg_clv,

    -- Marginal effects
    ROUND(
        (avg_transactions
         - LAG(avg_transactions) OVER (ORDER BY avg_credit_score))::numeric
    , 1) AS marginal_effect_transactions,

    ROUND(
        (avg_balance
         - LAG(avg_balance) OVER (ORDER BY avg_credit_score))::numeric
    , 2) AS marginal_effect_balance,

    ROUND(
        (avg_approval_rate_pct
         - LAG(avg_approval_rate_pct) OVER (ORDER BY avg_credit_score))::numeric
    , 2) AS marginal_effect_approval_pct,

    -- CLV change per 100 credit points
    ROUND(
        (
            avg_clv
            - LAG(avg_clv) OVER (ORDER BY avg_credit_score)
        )
        /
        NULLIF(
            avg_credit_score
            - LAG(avg_credit_score) OVER (ORDER BY avg_credit_score),
            0
        )
        * 100
    , 2) AS clv_change_per_100_credit_points,

    CURRENT_TIMESTAMP AS analyzed_at

FROM credit_score_band_agg
ORDER BY avg_credit_score