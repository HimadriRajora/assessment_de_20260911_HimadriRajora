{{
    config(
        materialized='view'
    )
}}

/*
    Staging: one row per city per day, typed and renamed.

    This is the only place that knows the shape of the API payload. Everything
    downstream sees clean columns, so an API field rename is a one-file change.
*/

with raw_weather as (

    select * from {{ source('raw', 'weather_daily') }}

),

parsed as (

    select
        city_id,
        city_name,
        country_code,
        weather_date,

        cast(payload ->> 'temperature_2m_max'  as double) as temperature_max_c,
        cast(payload ->> 'temperature_2m_min'  as double) as temperature_min_c,
        cast(payload ->> 'temperature_2m_mean' as double) as temperature_mean_c,
        cast(payload ->> 'precipitation_sum'   as double) as precipitation_mm,
        cast(payload ->> 'wind_speed_10m_max'  as double) as wind_speed_max_kmh,

        cast(api_meta ->> 'latitude'  as double) as latitude,
        cast(api_meta ->> 'longitude' as double) as longitude,
        cast(api_meta ->> 'elevation' as double) as elevation_m,

        _batch_id,
        _extracted_at,
        _loaded_at

    from raw_weather

)

select
    -- Surrogate key for the grain, so tests can assert the grain directly.
    city_id || '|' || cast(weather_date as varchar) as weather_key,
    city_id,
    city_name,
    country_code,
    weather_date,
    temperature_max_c,
    temperature_min_c,
    temperature_mean_c,
    round(temperature_max_c - temperature_min_c, 1) as temperature_range_c,
    precipitation_mm,
    wind_speed_max_kmh,
    latitude,
    longitude,
    elevation_m,
    _batch_id,
    _extracted_at,
    _loaded_at
from parsed
