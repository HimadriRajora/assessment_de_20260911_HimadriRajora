-- Every city should be present for every day we hold. A gap means a city's
-- extract failed while the run was still marked successful.
with days as (
    select distinct weather_date from {{ ref('mart_city_weather_daily') }}
),
cities as (
    select distinct city_id from {{ ref('mart_city_weather_daily') }}
),
expected as (
    select cities.city_id, days.weather_date from cities cross join days
)
select expected.*
from expected
left join {{ ref('mart_city_weather_daily') }} as actual
    on  actual.city_id = expected.city_id
    and actual.weather_date = expected.weather_date
where actual.city_id is null
