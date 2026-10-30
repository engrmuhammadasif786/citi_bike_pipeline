{{ config(materialized='view') }}

select
    *,
    case
        when trip_duration_minutes < 10 then 'Short (< 10 min)'
        when trip_duration_minutes between 10 and 25 then 'Medium (10-25 min)'
        when trip_duration_minutes between 25 and 45 then 'Long (25-45 min)'
        else 'Extended (> 45 min)'
    end as duration_tier,
    case
        when trip_distance_km > 0 and trip_duration_minutes > 0
        then round((trip_distance_km / (trip_duration_minutes / 60.0)), 2)
        else 0.0
    end as avg_speed_kmh
from {{ ref('stg_citibike_trips') }}
