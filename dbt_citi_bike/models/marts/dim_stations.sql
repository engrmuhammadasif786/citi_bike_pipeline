{{
    config(
        materialized='table',
        liquid_clustered_by=['station_id']
    )
}}

select
    start_station_id as station_id,
    max(start_station_name) as station_name,
    round(avg(start_lat), 6) as station_lat,
    round(avg(start_lng), 6) as station_lng,
    count(ride_id) as total_departures,
    round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
    round(avg(trip_distance_km), 3) as avg_distance_km,
    round(
        sum(case when rider_type = 'Annual Member' then 1 else 0 end) * 100.0 / count(ride_id),
        1
    ) as member_share_pct,
    round(
        sum(case when rideable_type_clean = 'Electric Bike' then 1 else 0 end) * 100.0 / count(ride_id),
        1
    ) as ebike_share_pct
from {{ ref('int_trips_enriched') }}
where start_station_id != 'UNKNOWN'
group by start_station_id
