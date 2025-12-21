-- ===================================================================
-- Custom dbt Macros for Financial Analytics
-- ===================================================================

-- Macro: Generate surrogate key (if dbt_utils not available)
{% macro generate_surrogate_key(field_list) %}
    MD5(
        {% for field in field_list %}
            COALESCE(CAST({{ field }} AS VARCHAR), '')
            {% if not loop.last %} || '|' || {% endif %}
        {% endfor %}
    )
{% endmacro %}

-- Macro: Calculate business days between two dates
{% macro business_days_between(start_date, end_date) %}
    (
        SELECT COUNT(*)
        FROM {{ ref('dim_date') }}
        WHERE date_actual BETWEEN {{ start_date }} AND {{ end_date }}
          AND is_weekday = TRUE
          AND is_holiday = FALSE
    )
{% endmacro %}

-- Macro: Pivot table for category analysis
{% macro pivot_category_metrics(fact_table, category_column, metric_column) %}
    SELECT
        date_key,
        {% for category in ['Grocery', 'Restaurant', 'Gas Station', 'Retail', 'Entertainment'] %}
        SUM(CASE WHEN {{ category_column }} = '{{ category }}' THEN {{ metric_column }} ELSE 0 END) AS {{ category | replace(' ', '_') | lower }}
        {% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ fact_table }}
    GROUP BY date_key
{% endmacro %}

-- Macro: Calculate moving average
{% macro moving_average(column_name, partition_by, order_by, window_size) %}
    AVG({{ column_name }}) OVER (
        {% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ window_size - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}

-- Macro: Calculate year-over-year growth
{% macro yoy_growth(current_value, previous_value) %}
    CASE 
        WHEN {{ previous_value }} = 0 OR {{ previous_value }} IS NULL THEN NULL
        ELSE ROUND((({{ current_value }} - {{ previous_value }})::NUMERIC / {{ previous_value }} * 100), 2)
    END
{% endmacro %}

-- Macro: Risk score banding
{% macro risk_score_band(score_column) %}
    CASE
        WHEN {{ score_column }} >= 0.7 THEN 'High Risk'
        WHEN {{ score_column }} >= 0.4 THEN 'Medium Risk'
        WHEN {{ score_column }} >= 0.2 THEN 'Low Risk'
        ELSE 'Very Low Risk'
    END
{% endmacro %}

-- Macro: Calculate RFM score components
{% macro calculate_rfm_score() %}
    WITH customer_rfm AS (
        SELECT
            customer_key,
            CURRENT_DATE - MAX(transaction_date)::DATE AS recency_days,
            COUNT(DISTINCT transaction_id) AS frequency,
            SUM(transaction_amount_abs) AS monetary
        FROM {{ ref('fact_transactions') }}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY customer_key
    ),
    rfm_scores AS (
        SELECT
            customer_key,
            NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
            NTILE(5) OVER (ORDER BY frequency) AS f_score,
            NTILE(5) OVER (ORDER BY monetary) AS m_score
        FROM customer_rfm
    )
    SELECT
        customer_key,
        r_score,
        f_score,
        m_score,
        (r_score + f_score + m_score) AS rfm_total_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
            WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
            ELSE 'Potential Loyalists'
        END AS rfm_segment
    FROM rfm_scores
{% endmacro %}

-- Macro: Safe division to avoid divide by zero
{% macro safe_divide(numerator, denominator) %}
    CASE 
        WHEN {{ denominator }} = 0 OR {{ denominator }} IS NULL THEN NULL
        ELSE {{ numerator }}::NUMERIC / {{ denominator }}
    END
{% endmacro %}

-- Macro: Calculate percentile rank
{% macro percentile_rank(column_name, partition_by=None) %}
    PERCENT_RANK() OVER (
        {% if partition_by %}PARTITION BY {{ partition_by }}{% endif %}
        ORDER BY {{ column_name }}
    )
{% endmacro %}

-- Macro: Format currency
{% macro format_currency(amount_column) %}
    '$' || TO_CHAR({{ amount_column }}, 'FM999,999,999.00')
{% endmacro %}

-- Macro: Data quality check summary
{% macro data_quality_check(table_name, primary_key) %}
    SELECT
        '{{ table_name }}' AS table_name,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT {{ primary_key }}) AS unique_keys,
        COUNT(*) - COUNT(DISTINCT {{ primary_key }}) AS duplicate_keys,
        SUM(CASE WHEN {{ primary_key }} IS NULL THEN 1 ELSE 0 END) AS null_keys,
        MAX(updated_at) AS last_updated
    FROM {{ ref(table_name) }}
{% endmacro %}

-- Macro: Generate test for accepted value range
{% macro test_value_range(model, column_name, min_value, max_value) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < {{ min_value }}
       OR {{ column_name }} > {{ max_value }}
{% endmacro %}

-- Macro: Create indexes post-model creation
{% macro create_indexes(table_name, index_columns) %}
    {% for column in index_columns %}
        CREATE INDEX IF NOT EXISTS idx_{{ table_name }}_{{ column | replace('.', '_') }}
        ON {{ this }} ({{ column }});
    {% endfor %}
{% endmacro %}

-- Macro: Grant permissions to roles
{% macro grant_select_to_role(role_name) %}
    {% if target.name == 'prod' %}
        GRANT SELECT ON {{ this }} TO {{ role_name }};
    {% endif %}
{% endmacro %}

-- Macro: Log model execution
{% macro log_model_execution() %}
    {% if execute %}
        {% do log("Running model: " ~ this.name ~ " in schema: " ~ this.schema, info=True) %}
        {% do log("Target: " ~ target.name, info=True) %}
    {% endif %}
{% endmacro %}

-- Macro: Calculate customer lifetime value
{% macro calculate_ltv(avg_purchase_value, purchase_frequency, customer_lifespan_years) %}
    {{ avg_purchase_value }} * {{ purchase_frequency }} * {{ customer_lifespan_years }} * 12
{% endmacro %}

-- Macro: Anonymize PII data (for non-prod environments)
{% macro anonymize_pii(column_name, column_type='email') %}
    {% if target.name != 'prod' %}
        {% if column_type == 'email' %}
            'user_' || MD5({{ column_name }})::VARCHAR || '@example.com'
        {% elif column_type == 'phone' %}
            '555-' || RIGHT(MD5({{ column_name }})::VARCHAR, 7)
        {% elif column_type == 'ssn' %}
            'XXX-XX-' || RIGHT(MD5({{ column_name }})::VARCHAR, 4)
        {% else %}
            MD5({{ column_name }})::VARCHAR
        {% endif %}
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}