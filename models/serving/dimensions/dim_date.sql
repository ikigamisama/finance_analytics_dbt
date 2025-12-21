{{
    config(
        materialized='table',
        tags=['gold', 'dimension', 'serving', 'date']
    )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
       )
    }}
),

date_dimension AS (
    SELECT
        -- Primary Key
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_key,
        date_day AS date_actual,
        
        -- Year Attributes
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        'Q' || EXTRACT(QUARTER FROM date_day) AS quarter_name,
        EXTRACT(YEAR FROM date_day) || '-Q' || EXTRACT(QUARTER FROM date_day) AS year_quarter,
        
        -- Month Attributes
        EXTRACT(MONTH FROM date_day) AS month_number,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        
        -- Week Attributes
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(DOW FROM date_day) AS day_of_week,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,
        
        -- Day Attributes
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        
        -- Week Flags
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DOW FROM date_day) BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- Month Flags
        CASE WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
        CASE WHEN EXTRACT(DAY FROM date_day) = EXTRACT(DAY FROM (date_day + INTERVAL '1 month - 1 day')) THEN TRUE ELSE FALSE END AS is_month_end,
        
        -- Quarter Flags
        CASE WHEN EXTRACT(MONTH FROM date_day) IN (1, 4, 7, 10) AND EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_quarter_start,
        
        -- Year Flags
        CASE WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_year_start,
        CASE WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 31 THEN TRUE ELSE FALSE END AS is_year_end,
        
        -- US Holidays (simplified)
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN 'New Year''s Day'
            WHEN EXTRACT(MONTH FROM date_day) = 7 AND EXTRACT(DAY FROM date_day) = 4 THEN 'Independence Day'
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 25 THEN 'Christmas'
            ELSE NULL
        END AS holiday_name,
        
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN TRUE
            WHEN EXTRACT(MONTH FROM date_day) = 7 AND EXTRACT(DAY FROM date_day) = 4 THEN TRUE
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 25 THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        
        -- Relative Flags
        CASE WHEN date_day = CURRENT_DATE THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = CURRENT_DATE - INTERVAL '1 day' THEN TRUE ELSE FALSE END AS is_yesterday,
        
        -- Fiscal Period (assuming fiscal year = calendar year)
        EXTRACT(YEAR FROM date_day) AS fiscal_year,
        EXTRACT(QUARTER FROM date_day) AS fiscal_quarter,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dw_created_at
        
    FROM date_spine
)

SELECT * FROM date_dimension