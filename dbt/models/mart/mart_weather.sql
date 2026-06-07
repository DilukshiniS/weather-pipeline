{{
  config(
    materialized='table',
    description='Final analytics-ready weather table for all cities'
  )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

final AS (
    SELECT
        WEATHER_DATE,
        LOAD_DATE,
        CITY_NAME,
        COUNTRY,
        LATITUDE,
        LONGITUDE,

        temp_max_c,
        temp_min_c,
        ROUND((temp_max_c + temp_min_c) / 2, 1)   AS temp_avg_c,
        ROUND(temp_max_c - temp_min_c, 1)           AS temp_range_c,
        precipitation_mm,

        CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END AS did_rain,

        CASE
            WHEN (temp_max_c + temp_min_c) / 2 >= 35 THEN 'Very Hot'
            WHEN (temp_max_c + temp_min_c) / 2 >= 30 THEN 'Hot'
            WHEN (temp_max_c + temp_min_c) / 2 >= 25 THEN 'Warm'
            WHEN (temp_max_c + temp_min_c) / 2 >= 15 THEN 'Comfortable'
            WHEN (temp_max_c + temp_min_c) / 2 >= 5  THEN 'Cool'
            ELSE 'Cold'
        END AS temp_category,

        INSERTED_AT

    FROM staging
)

SELECT * FROM final
ORDER BY WEATHER_DATE DESC, CITY_NAME ASC