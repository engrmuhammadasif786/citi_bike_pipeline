{{ config(materialized='view') }}

with source as (
    select * from {{ source('silver', 'clean_trips') }}
)

select
    ride_id,
    started_at_ts,
    ended_at_ts,
    trip_duration_seconds,
    trip_duration_minutes,
    started_date,
    trip_year_month,
    start_hour,
    day_of_week_num,
    day_of_week_name,
    is_weekend,
    time_of_day_bucket,
    rideable_type_clean,
    rider_type,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    trip_distance_km,
    is_round_trip
from source
where trip_duration_seconds between 60 and 86400
