-- This test fails if any row has TEMP_MIN_C > TEMP_MAX_C
-- dbt tests pass when the query returns 0 rows
-- So we SELECT rows that VIOLATE the rule

SELECT
    CITY_NAME,
    WEATHER_DATE,
    TEMP_MIN_C,
    TEMP_MAX_C
FROM {{ ref('fct_weather_daily') }}
WHERE TEMP_MIN_C > TEMP_MAX_C