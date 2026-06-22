{{
  config(
    materialized='table',
    schema='MART',
    description='Date dimension table - one row per date'
  )
}}

SELECT DISTINCT
    WEATHER_DATE                                    AS date_day,
    YEAR(WEATHER_DATE)                              AS year,
    MONTH(WEATHER_DATE)                             AS month,
    DAY(WEATHER_DATE)                               AS day,
    DAYOFWEEK(WEATHER_DATE)                         AS day_of_week,
    DAYNAME(WEATHER_DATE)                           AS day_name,
    MONTHNAME(WEATHER_DATE)                         AS month_name,
    CASE
        WHEN DAYOFWEEK(WEATHER_DATE) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                             AS is_weekend,
    QUARTER(WEATHER_DATE)                           AS quarter
FROM {{ ref('stg_weather') }}
ORDER BY date_day