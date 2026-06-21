{{
  config(
    materialized='incremental',
    unique_key=['CITY_NAME', 'LOAD_DATE'],
    description='Daily aggregated weather summary per city - keeps full history'
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
),

final AS (
    SELECT
        LOAD_DATE,
        CITY_NAME,
        COUNTRY,
        LATITUDE,
        LONGITUDE,
        ROUND(AVG(temp_max_c), 1)                        AS avg_temp_max_c,
        ROUND(AVG(temp_min_c), 1)                        AS avg_temp_min_c,
        ROUND(AVG((temp_max_c + temp_min_c) / 2), 1)     AS avg_temp_c,
        MAX(temp_max_c)                                   AS week_high_c,
        MIN(temp_min_c)                                   AS week_low_c,
        ROUND(SUM(precipitation_mm), 1)                  AS total_precipitation_mm,
        SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) AS rainy_days,
        COUNT(*)                                          AS days_forecasted,
        CASE
            WHEN AVG((temp_max_c + temp_min_c) / 2) >= 35 THEN 'Very Hot'
            WHEN AVG((temp_max_c + temp_min_c) / 2) >= 30 THEN 'Hot'
            WHEN AVG((temp_max_c + temp_min_c) / 2) >= 25 THEN 'Warm'
            WHEN AVG((temp_max_c + temp_min_c) / 2) >= 15 THEN 'Comfortable'
            WHEN AVG((temp_max_c + temp_min_c) / 2) >= 5  THEN 'Cool'
            ELSE 'Cold'
        END AS week_temp_category
    FROM filtered
    GROUP BY LOAD_DATE, CITY_NAME, COUNTRY, LATITUDE, LONGITUDE
)

SELECT * FROM final
ORDER BY LOAD_DATE DESC, CITY_NAME ASC