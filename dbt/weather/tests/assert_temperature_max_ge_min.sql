-- A day's maximum cannot be below its minimum. Catches a column swap in
-- staging or a units change on the API side.
select *
from {{ ref('stg_weather_daily') }}
where temperature_max_c < temperature_min_c
