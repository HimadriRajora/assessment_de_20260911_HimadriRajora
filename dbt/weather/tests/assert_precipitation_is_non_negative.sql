-- Precipitation and wind speed are physically non-negative.
select *
from {{ ref('stg_weather_daily') }}
where precipitation_mm < 0
   or wind_speed_max_kmh < 0
