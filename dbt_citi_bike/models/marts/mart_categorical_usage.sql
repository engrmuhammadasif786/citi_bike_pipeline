{{
    config(
        materialized='table',
        partition_by=['trip_year_month'],
        liquid_clustered_by=['rider_type', 'rideable_type_clean']
    )
}}

select
    trip_year_month,
    rider_type,
    rideable_type_clean,
    time_of_day_bucket,
    is_weekend,
    count(ride_id) as total_trips,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    round(avg(trip_distance_km), 3) as avg_distance_km,
    sum(case when is_round_trip then 1 else 0 end) as round_trip_count
from {{ ref('int_trips_enriched') }}
group by
    trip_year_month,
    rider_type,
    rideable_type_clean,
    time_of_day_bucket,
    is_weekend
