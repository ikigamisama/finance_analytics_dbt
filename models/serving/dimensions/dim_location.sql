{{
    config(
        materialized='table',
        schema="gold",
        tags=['gold', 'dimension', 'serving', 'location']
    )
}}
WITH branches AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['branch_id']) }} AS location_key,
        branch_id AS location_natural_key,
        'BRANCH' AS location_type,
        branch_name AS location_name,
        branch_code AS location_code,
        address,
        city,
        state,
        zip_code,
        country,
        latitude,
        longitude,
        region,
        phone,
        is_active::BOOLEAN AS is_active,
        CAST(NULL AS BOOLEAN) AS is_operational,
        CAST(NULL AS BOOLEAN) AS is_24_hour
    FROM {{ ref('stg_branch_locations') }}
),

atms AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['atm_id']) }} AS location_key,
        atm_id AS location_natural_key,
        'ATM' AS location_type,
        location_name,
        atm_code AS location_code,
        address,
        city,
        state,
        zip_code,
        country,
        latitude,
        longitude,
        CAST(NULL AS TEXT) AS region,
        CAST(NULL AS TEXT) AS phone,
        CAST(NULL AS BOOLEAN) AS is_active,
        is_operational::BOOLEAN AS is_operational,
        is_24_hour::BOOLEAN AS is_24_hour
    FROM {{ ref('stg_atm_locations') }}
),

combined_locations AS (
    SELECT * FROM branches
    UNION ALL
    SELECT * FROM atms
)

SELECT 
    *,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM combined_locations
