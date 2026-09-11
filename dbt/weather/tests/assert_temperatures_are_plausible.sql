-- Guard against silently ingesting a sentinel value (-9999) or a unit switch
-- from Celsius to Fahrenheit or Kelvin.
select *
from {{ ref('stg_weather_daily') }}
where temperature_mean_c not between -70 and 60
   or temperature_max_c  not between -70 and 60
   or temperature_min_c  not between -70 and 60
