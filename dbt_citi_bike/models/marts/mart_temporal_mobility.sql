{{
    config(
        materialized='table',
        partition_by=['trip_year_month'],
        liquid_clustered_by=['started_date', 'rider_type']
    )
}}

select
    started_date,
    trip_year_month,
    start_hour,
    day_of_week_num,
    day_of_week_name,
    is_weekend,
    rider_type,
    rideable_type_clean,
    count(ride_id) as total_trips,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    round(avg(trip_distance_km), 3) as avg_distance_km
from {{ ref('int_trips_enriched') }}
group by
    started_date,
    trip_year_month,
    start_hour,
    day_of_week_num,
    day_of_week_name,
    is_weekend,
    rider_type,
    rideable_type_clean
