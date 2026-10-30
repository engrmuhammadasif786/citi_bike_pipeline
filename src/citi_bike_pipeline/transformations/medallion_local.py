"""Local Medallion execution engine using DuckDB + Partitioned Parquet.

Provides 100% parity with the Databricks PySpark Medallion pipeline so peer reviewers
and CI/CD tests can run the entire Bronze -> Silver -> Gold pipeline locally without
requiring AWS or Databricks credentials.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from citi_bike_pipeline.config import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QualityCheckResult:
    """Outcome of a single data quality assertion."""

    check_name: str
    passed: bool
    metric_value: float
    details: str


class LocalMedallionPipeline:
    """Executes Bronze -> Silver -> Gold Medallion layers using DuckDB and Parquet."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.config.ensure_local_dirs()

    def _connect(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(str(self.config.duckdb_path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        return conn

    def run_bronze(self, raw_csv_files: list[Path] | None = None) -> int:
        """Ingest raw CSV files into `bronze.raw_trips` and write `data/bronze/raw_trips.parquet`."""
        csv_paths = raw_csv_files or sorted(self.config.raw_dir.glob("*.csv"))
        if not csv_paths:
            raise FileNotFoundError(f"No raw CSV files found in {self.config.raw_dir}")

        file_list_sql = ", ".join(f"'{p.as_posix()}'" for p in csv_paths)
        bronze_parquet = self.config.bronze_dir / "raw_trips.parquet"

        with self._connect() as conn:
            conn.execute(
                f"""
                CREATE OR REPLACE TABLE bronze.raw_trips AS
                SELECT
                    CAST(ride_id AS VARCHAR) AS ride_id,
                    CAST(rideable_type AS VARCHAR) AS rideable_type,
                    CAST(started_at AS VARCHAR) AS started_at,
                    CAST(ended_at AS VARCHAR) AS ended_at,
                    CAST(start_station_name AS VARCHAR) AS start_station_name,
                    CAST(start_station_id AS VARCHAR) AS start_station_id,
                    CAST(end_station_name AS VARCHAR) AS end_station_name,
                    CAST(end_station_id AS VARCHAR) AS end_station_id,
                    TRY_CAST(start_lat AS DOUBLE) AS start_lat,
                    TRY_CAST(start_lng AS DOUBLE) AS start_lng,
                    TRY_CAST(end_lat AS DOUBLE) AS end_lat,
                    TRY_CAST(end_lng AS DOUBLE) AS end_lng,
                    CAST(member_casual AS VARCHAR) AS member_casual,
                    CURRENT_TIMESTAMP AS _ingested_at,
                    filename AS _source_file
                FROM read_csv(
                    [{file_list_sql}],
                    header=true,
                    filename=true,
                    all_varchar=true,
                    ignore_errors=true
                );
                """
            )
            conn.execute(
                f"COPY bronze.raw_trips TO '{bronze_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )
            row_count = conn.execute("SELECT COUNT(*) FROM bronze.raw_trips").fetchone()[0]

        logger.info("Bronze layer built with %d raw rows at %s", row_count, bronze_parquet)
        return int(row_count)

    def run_silver(self) -> int:
        """Clean, deduplicate, enrich, partition by `trip_year_month`, and cluster Silver layer."""
        silver_parquet = self.config.silver_dir / "clean_trips.parquet"
        silver_partitioned_dir = self.config.silver_dir / "clean_trips_partitioned"
        silver_partitioned_dir.mkdir(parents=True, exist_ok=True)

        with self._connect() as conn:
            conn.execute(
                """
                CREATE OR REPLACE TABLE silver.clean_trips AS
                WITH deduplicated AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY _ingested_at DESC) AS _rn
                    FROM bronze.raw_trips
                    WHERE ride_id IS NOT NULL
                      AND started_at IS NOT NULL
                      AND ended_at IS NOT NULL
                ),
                typed AS (
                    SELECT
                        ride_id,
                        rideable_type,
                        TRY_CAST(started_at AS TIMESTAMP) AS started_at_ts,
                        TRY_CAST(ended_at AS TIMESTAMP) AS ended_at_ts,
                        COALESCE(NULLIF(TRIM(start_station_name), ''), 'Unknown Station') AS start_station_name,
                        COALESCE(NULLIF(TRIM(start_station_id), ''), 'UNKNOWN') AS start_station_id,
                        COALESCE(NULLIF(TRIM(end_station_name), ''), 'Unknown Station') AS end_station_name,
                        COALESCE(NULLIF(TRIM(end_station_id), ''), 'UNKNOWN') AS end_station_id,
                        start_lat,
                        start_lng,
                        end_lat,
                        end_lng,
                        LOWER(TRIM(member_casual)) AS member_casual,
                        _ingested_at,
                        _source_file
                    FROM deduplicated
                    WHERE _rn = 1
                ),
                enriched AS (
                    SELECT
                        ride_id,
                        started_at_ts,
                        ended_at_ts,
                        DATE_DIFF('second', started_at_ts, ended_at_ts) AS trip_duration_seconds,
                        ROUND(DATE_DIFF('second', started_at_ts, ended_at_ts) / 60.0, 2) AS trip_duration_minutes,
                        CAST(started_at_ts AS DATE) AS started_date,
                        STRFTIME(started_at_ts, '%Y-%m') AS trip_year_month,
                        EXTRACT('hour' FROM started_at_ts) AS start_hour,
                        EXTRACT('dow' FROM started_at_ts) AS day_of_week_num,
                        STRFTIME(started_at_ts, '%A') AS day_of_week_name,
                        CASE WHEN EXTRACT('dow' FROM started_at_ts) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                        CASE
                            WHEN EXTRACT('hour' FROM started_at_ts) BETWEEN 6 AND 9 THEN 'Morning Rush (06-09)'
                            WHEN EXTRACT('hour' FROM started_at_ts) BETWEEN 10 AND 15 THEN 'Midday (10-15)'
                            WHEN EXTRACT('hour' FROM started_at_ts) BETWEEN 16 AND 19 THEN 'Evening Rush (16-19)'
                            ELSE 'Night / Off-Peak (20-05)'
                        END AS time_of_day_bucket,
                        CASE
                            WHEN rideable_type = 'electric_bike' THEN 'Electric Bike'
                            WHEN rideable_type = 'classic_bike' THEN 'Classic Bike'
                            WHEN rideable_type = 'docked_bike' THEN 'Docked Bike'
                            ELSE 'Other Bike'
                        END AS rideable_type_clean,
                        CASE
                            WHEN member_casual = 'member' THEN 'Annual Member'
                            ELSE 'Casual Rider'
                        END AS rider_type,
                        start_station_id,
                        start_station_name,
                        end_station_id,
                        end_station_name,
                        start_lat,
                        start_lng,
                        end_lat,
                        end_lng,
                        ROUND(
                            2.0 * 6371.0 * ASIN(
                                SQRT(
                                    POWER(SIN(RADIANS(end_lat - start_lat) / 2.0), 2) +
                                    COS(RADIANS(start_lat)) * COS(RADIANS(end_lat)) *
                                    POWER(SIN(RADIANS(end_lng - start_lng) / 2.0), 2)
                                )
                            ),
                            3
                        ) AS trip_distance_km,
                        CASE
                            WHEN start_station_id != 'UNKNOWN' AND start_station_id = end_station_id THEN TRUE
                            ELSE FALSE
                        END AS is_round_trip,
                        _ingested_at
                    FROM typed
                    WHERE started_at_ts IS NOT NULL
                      AND ended_at_ts IS NOT NULL
                      AND start_lat BETWEEN 40.4 AND 41.0
                      AND start_lng BETWEEN -74.3 AND -73.6
                      AND end_lat BETWEEN 40.4 AND 41.0
                      AND end_lng BETWEEN -74.3 AND -73.6
                )
                SELECT *
                FROM enriched
                WHERE trip_duration_seconds BETWEEN 60 AND 86400
                -- Order by partition and clustering keys (simulating Delta Z-ORDER co-location)
                ORDER BY trip_year_month, start_station_id, started_date, rider_type;
                """
            )

            conn.execute(
                f"COPY silver.clean_trips TO '{silver_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )
            conn.execute(
                f"""
                COPY silver.clean_trips TO '{silver_partitioned_dir.as_posix()}'
                (FORMAT PARQUET, PARTITION_BY (trip_year_month), OVERWRITE_OR_IGNORE true);
                """
            )
            row_count = conn.execute("SELECT COUNT(*) FROM silver.clean_trips").fetchone()[0]

        logger.info("Silver layer built with %d clean rows at %s", row_count, silver_parquet)
        return int(row_count)

    def run_gold(self) -> dict[str, int]:
        """Build Gold dimensional and analytical marts for the BI dashboard."""
        cat_parquet = self.config.gold_dir / "mart_categorical_usage.parquet"
        temp_parquet = self.config.gold_dir / "mart_temporal_mobility.parquet"
        station_parquet = self.config.gold_dir / "dim_stations.parquet"

        with self._connect() as conn:
            # 1. Categorical Distribution Mart (Dashboard Tile 1)
            conn.execute(
                """
                CREATE OR REPLACE TABLE gold.mart_categorical_usage AS
                SELECT
                    trip_year_month,
                    rider_type,
                    rideable_type_clean,
                    time_of_day_bucket,
                    is_weekend,
                    COUNT(ride_id) AS total_trips,
                    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
                    ROUND(AVG(trip_distance_km), 3) AS avg_distance_km,
                    SUM(CASE WHEN is_round_trip THEN 1 ELSE 0 END) AS round_trip_count
                FROM silver.clean_trips
                GROUP BY
                    trip_year_month,
                    rider_type,
                    rideable_type_clean,
                    time_of_day_bucket,
                    is_weekend
                ORDER BY trip_year_month, total_trips DESC;
                """
            )
            conn.execute(
                f"COPY gold.mart_categorical_usage TO '{cat_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )

            # 2. Temporal Mobility Mart (Dashboard Tile 2)
            conn.execute(
                """
                CREATE OR REPLACE TABLE gold.mart_temporal_mobility AS
                SELECT
                    started_date,
                    trip_year_month,
                    start_hour,
                    day_of_week_num,
                    day_of_week_name,
                    is_weekend,
                    rider_type,
                    rideable_type_clean,
                    COUNT(ride_id) AS total_trips,
                    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
                    ROUND(AVG(trip_distance_km), 3) AS avg_distance_km
                FROM silver.clean_trips
                GROUP BY
                    started_date,
                    trip_year_month,
                    start_hour,
                    day_of_week_num,
                    day_of_week_name,
                    is_weekend,
                    rider_type,
                    rideable_type_clean
                ORDER BY started_date, start_hour;
                """
            )
            conn.execute(
                f"COPY gold.mart_temporal_mobility TO '{temp_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )

            # 3. Station Dimension & Geospatial Mart (Dashboard Bonus Tile 3)
            conn.execute(
                """
                CREATE OR REPLACE TABLE gold.dim_stations AS
                SELECT
                    start_station_id AS station_id,
                    ANY_VALUE(start_station_name) AS station_name,
                    ROUND(AVG(start_lat), 6) AS station_lat,
                    ROUND(AVG(start_lng), 6) AS station_lng,
                    COUNT(ride_id) AS total_departures,
                    ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
                    ROUND(AVG(trip_distance_km), 3) AS avg_distance_km,
                    ROUND(
                        SUM(CASE WHEN rider_type = 'Annual Member' THEN 1 ELSE 0 END) * 100.0 / COUNT(ride_id),
                        1
                    ) AS member_share_pct,
                    ROUND(
                        SUM(CASE WHEN rideable_type_clean = 'Electric Bike' THEN 1 ELSE 0 END) * 100.0 / COUNT(ride_id),
                        1
                    ) AS ebike_share_pct
                FROM silver.clean_trips
                WHERE start_station_id != 'UNKNOWN'
                GROUP BY start_station_id
                ORDER BY total_departures DESC;
                """
            )
            conn.execute(
                f"COPY gold.dim_stations TO '{station_parquet.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
            )

            counts = {
                "mart_categorical_usage": int(
                    conn.execute("SELECT COUNT(*) FROM gold.mart_categorical_usage").fetchone()[0]
                ),
                "mart_temporal_mobility": int(
                    conn.execute("SELECT COUNT(*) FROM gold.mart_temporal_mobility").fetchone()[0]
                ),
                "dim_stations": int(
                    conn.execute("SELECT COUNT(*) FROM gold.dim_stations").fetchone()[0]
                ),
            }

        logger.info("Gold marts created: %s", counts)
        return counts

    def run_quality_checks(self) -> list[QualityCheckResult]:
        """Execute automated data quality gates across Silver and Gold layers."""
        results: list[QualityCheckResult] = []
        with self._connect() as conn:
            silver_count = int(
                conn.execute("SELECT COUNT(*) FROM silver.clean_trips").fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    check_name="silver_non_empty",
                    passed=silver_count > 0,
                    metric_value=float(silver_count),
                    details=f"Silver clean_trips contains {silver_count:,} valid rides",
                )
            )

            dup_count = int(
                conn.execute(
                    "SELECT COUNT(*) - COUNT(DISTINCT ride_id) FROM silver.clean_trips"
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    check_name="silver_unique_ride_id",
                    passed=dup_count == 0,
                    metric_value=float(dup_count),
                    details=f"Duplicate ride_id count: {dup_count}",
                )
            )

            invalid_duration = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM silver.clean_trips
                    WHERE trip_duration_seconds < 60 OR trip_duration_seconds > 86400
                    """
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    check_name="silver_valid_duration_bounds",
                    passed=invalid_duration == 0,
                    metric_value=float(invalid_duration),
                    details=f"Out-of-bounds duration rows: {invalid_duration}",
                )
            )

            silver_sum = int(conn.execute("SELECT COUNT(*) FROM silver.clean_trips").fetchone()[0])
            gold_cat_sum = int(
                conn.execute(
                    "SELECT COALESCE(SUM(total_trips), 0) FROM gold.mart_categorical_usage"
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    check_name="gold_reconciles_with_silver",
                    passed=silver_sum == gold_cat_sum and gold_cat_sum > 0,
                    metric_value=float(abs(silver_sum - gold_cat_sum)),
                    details=f"Silver trips ({silver_sum:,}) == Gold categorical sum ({gold_cat_sum:,})",
                )
            )

        for res in results:
            if not res.passed:
                raise AssertionError(f"Data quality check failed: {res.check_name} ({res.details})")
        return results
