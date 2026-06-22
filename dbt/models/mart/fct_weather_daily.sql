{{
  config(
    materialized='incremental',
    unique_key=['CITY_NAME', 'WEATHER_DATE', 'LOAD_DATE'],
    schema='MART',
    description='Daily weather fact table - one row per city per forecast date'
  )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

filtered AS (
    SELECT * FROM staging
    {% if is_incremental() %}
        WHERE LOAD_DATE > COALESCE(
            (SELECT MAX(LOAD_DATE) FROM {{ this }}),
            '2000-01-01'
        )
    {% endif %}
)

SELECT
    LOAD_DATE,
    CITY_NAME,
    COUNTRY,
    WEATHER_DATE,
    TEMP_MAX_C,
    TEMP_MIN_C,
    ROUND((TEMP_MAX_C + TEMP_MIN_C) / 2, 1)        AS temp_avg_c,
    ROUND(TEMP_MAX_C - TEMP_MIN_C, 1)               AS temp_range_c,
    PRECIPITATION_MM,
    CASE WHEN PRECIPITATION_MM > 0 THEN TRUE ELSE FALSE END AS did_rain,
    CASE
        WHEN (TEMP_MAX_C + TEMP_MIN_C) / 2 >= 35 THEN 'Very Hot'
        WHEN (TEMP_MAX_C + TEMP_MIN_C) / 2 >= 30 THEN 'Hot'
        WHEN (TEMP_MAX_C + TEMP_MIN_C) / 2 >= 25 THEN 'Warm'
        WHEN (TEMP_MAX_C + TEMP_MIN_C) / 2 >= 15 THEN 'Comfortable'
        WHEN (TEMP_MAX_C + TEMP_MIN_C) / 2 >= 5  THEN 'Cool'
        ELSE 'Cold'
    END AS temp_category
FROM filtered
ORDER BY LOAD_DATE DESC, CITY_NAME ASC, WEATHER_DATE ASC