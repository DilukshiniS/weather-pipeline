{{
  config(
    materialized='view',
    description='Cleaned weather data from the RAW layer'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'weather_raw') }}
),

cleaned AS (
    SELECT
        LOAD_DATE,
        CITY_NAME,
        COUNTRY,
        LATITUDE,
        LONGITUDE,
        WEATHER_DATE,
        ROUND(TEMP_MAX, 1)             AS temp_max_c,
        ROUND(TEMP_MIN, 1)             AS temp_min_c,
        COALESCE(PRECIPITATION, 0.0)   AS precipitation_mm,
        INSERTED_AT
    FROM source
    WHERE WEATHER_DATE IS NOT NULL
      AND CITY_NAME IS NOT NULL
      AND TEMP_MAX IS NOT NULL
      AND TEMP_MIN IS NOT NULL
)

SELECT * FROM cleaned