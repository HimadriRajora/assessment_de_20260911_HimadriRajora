{{
    config(
        materialized='table'
    )
}}

/*
    Mart: the daily city weather report.

    Grain is unchanged from staging (one row per city per day) but the row is
    now answerable on its own: it carries the 7-day rolling average and the
    change since yesterday, which is what someone actually asks when they look
    at a weather table ("is this warm for the time of year?").
*/

with daily as (

    select * from {{ ref('stg_weather_daily') }}

),

enriched as (

    select
        weather_key,
        city_id,
        city_name,
        country_code,
        weather_date,
        temperature_max_c,
        temperature_min_c,
        temperature_mean_c,
        temperature_range_c,
        precipitation_mm,
        wind_speed_max_kmh,

        round(avg(temperature_mean_c) over (
            partition by city_id
            order by weather_date
            rows between 6 preceding and current row
        ), 1) as temperature_mean_7d_avg_c,

        round(
            temperature_mean_c - lag(temperature_mean_c) over (
                partition by city_id order by weather_date
            ),
            1
        ) as temperature_mean_change_c,

        precipitation_mm > 0.0 as is_rainy_day

    from daily

)

select * from enriched
