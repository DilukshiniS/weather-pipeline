{{
  config(
    materialized='table',
    schema='MART',
    description='City dimension table - one row per city'
  )
}}

SELECT DISTINCT
    CITY_NAME,
    COUNTRY,
    LATITUDE,
    LONGITUDE
FROM {{ ref('stg_weather') }}