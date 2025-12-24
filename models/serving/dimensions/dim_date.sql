{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'dimension', 'serving', 'date']
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    )}}
),

date_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
        date_day AS date_actual,
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'YYYY-Q') AS year_quarter,
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DOW FROM date_day) = 0 THEN TRUE ELSE FALSE END AS is_sunday,
        CASE WHEN EXTRACT(DOW FROM date_day) = 6 THEN TRUE ELSE FALSE END AS is_saturday,
        DATE_TRUNC('month', date_day)::DATE AS first_day_of_month,
        (DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,
        DATE_TRUNC('quarter', date_day)::DATE AS first_day_of_quarter,
        (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months - 1 day')::DATE AS last_day_of_quarter,
        DATE_TRUNC('year', date_day)::DATE AS first_day_of_year,
        (DATE_TRUNC('year', date_day) + INTERVAL '1 year - 1 day')::DATE AS last_day_of_year,
        CASE 
            WHEN date_day = DATE_TRUNC('month', date_day)::DATE THEN TRUE 
            ELSE FALSE 
        END AS is_first_day_of_month,
        CASE 
            WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month - 1 day')::DATE 
            THEN TRUE ELSE FALSE 
        END AS is_last_day_of_month
    FROM date_spine
)

SELECT * FROM date_dimension