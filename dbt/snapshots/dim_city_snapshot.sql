{% snapshot dim_city_snapshot %}

{{
    config(
        target_schema='MART',
        unique_key='CITY_NAME',
        strategy='check',
        check_cols=['COUNTRY', 'LATITUDE', 'LONGITUDE'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    CITY_NAME,
    COUNTRY,
    LATITUDE,
    LONGITUDE
FROM {{ ref('stg_weather') }}

{% endsnapshot %}