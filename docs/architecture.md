# Architecture & Data Modeling Deep Dive

## 1. Medallion Layer Specifications

### Bronze Layer (`bronze.raw_trips`)
- **Source:** Official Citi Bike monthly CSV archives (`JC-YYYYMM-citibike-tripdata.csv.zip` / `YYYYMM-citibike-tripdata.csv.zip`) landed in `s3://<bucket>/raw/citibike/tripdata/year_month=YYYYMM/`.
- **Behavior:** Schema-enforced ingestion preserving raw string timestamps and audit columns (`_ingested_at`, `_source_file`).

### Silver Layer (`silver.clean_trips`)
- **Transformations Applied:**
  1. **Deduplication:** `ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY _ingested_at DESC) = 1`.
  2. **Outlier & Anomaly Removal:** Removes false starts (`trip_duration_seconds < 60`), unreturned bikes (`trip_duration_seconds > 86400`), and invalid GPS coordinates outside the NYC/NJ bounding box (`lat IN [40.4, 41.0]`, `lng IN [-74.3, -73.6]`).
  3. **Feature Engineering:**
     - `trip_year_month` (`YYYY-MM`), `started_date`, `start_hour`, `day_of_week_name`, `is_weekend`
     - `time_of_day_bucket` (`Morning Rush (06-09)`, `Midday (10-15)`, `Evening Rush (16-19)`, `Night / Off-Peak (20-05)`)
     - `rideable_type_clean` (`Electric Bike`, `Classic Bike`, `Docked Bike`)
     - `rider_type` (`Annual Member`, `Casual Rider`)
     - `trip_distance_km` via the spherical Haversine formula
     - `is_round_trip` (`start_station_id == end_station_id`)
- **Storage Optimization:**
  - `PARTITIONED BY (trip_year_month)`
  - `OPTIMIZE silver.clean_trips ZORDER BY (start_station_id, started_date, member_casual)`

### Gold Layer (`gold.*`)
1. `gold.mart_categorical_usage`: Pre-aggregated metrics by `(trip_year_month, rider_type, rideable_type_clean, time_of_day_bucket, is_weekend)` powering Dashboard Tile 1.
2. `gold.mart_temporal_mobility`: Daily and hourly ride volumes, average durations, and distances by `(started_date, trip_year_month, start_hour, day_of_week_name, is_weekend, rider_type, rideable_type_clean)` powering Dashboard Tile 2.
3. `gold.dim_stations`: Station-level departure volumes, coordinates, average trip metrics, annual member share (%), and electric bike share (%) powering Dashboard Tile 3.
