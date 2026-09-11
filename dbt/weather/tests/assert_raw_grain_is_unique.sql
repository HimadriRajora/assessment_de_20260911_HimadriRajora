-- The loader promises one row per city per day. If a re-run ever appended
-- instead of replacing, this is the test that goes red first.
select
    city_id,
    weather_date,
    count(*) as row_count
from {{ source('raw', 'weather_daily') }}
group by city_id, weather_date
having count(*) > 1
