{{
    config(
        materialized='table'
    )
}}

/*
    Mart: one row per city summarising every day we hold.

    This is the "business user" view -- warmest city, wettest city, how many
    days it rained -- and it is what the notebook queries at the end.
*/

with daily as (

    select * from {{ ref('mart_city_weather_daily') }}

)

select
    city_id,
    city_name,
    country_code,
    count(*)                                   as days_observed,
    min(weather_date)                          as first_weather_date,
    max(weather_date)                          as last_weather_date,
    round(avg(temperature_mean_c), 1)          as avg_temperature_c,
    round(max(temperature_max_c), 1)           as warmest_day_c,
    round(min(temperature_min_c), 1)           as coldest_night_c,
    round(sum(precipitation_mm), 1)            as total_precipitation_mm,
    count(*) filter (where is_rainy_day)       as rainy_days,
    round(
        100.0 * count(*) filter (where is_rainy_day) / nullif(count(*), 0),
        1
    )                                          as rainy_day_pct,
    round(max(wind_speed_max_kmh), 1)          as windiest_day_kmh
from daily
group by city_id, city_name, country_code
