"""PySpark + Delta Lake Medallion transformations for Databricks Unity Catalog.

Warehouse Optimization Design:
- Partitioning: `trip_year_month` (e.g., `2024-01`) keeps partition cardinality low (~12 partitions/year),
  preventing the small-file problem while enabling partition pruning on monthly/quarterly date range filters.
- Clustering / Z-Ordering: `ZORDER BY (start_station_id, started_date, member_casual)` co-locates data blocks
  for high-cardinality station lookups, daily time-series aggregations, and rider-segment slices used by the
  downstream BI dashboard.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from citi_bike_pipeline.config import PipelineConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def get_citibike_spark_schema():
    """Return explicit PySpark StructType for raw Citi Bike tripdata CSV ingestion."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("ride_id", StringType(), nullable=False),
            StructField("rideable_type", StringType(), nullable=True),
            StructField("started_at", StringType(), nullable=False),
            StructField("ended_at", StringType(), nullable=False),
            StructField("start_station_name", StringType(), nullable=True),
            StructField("start_station_id", StringType(), nullable=True),
            StructField("end_station_name", StringType(), nullable=True),
            StructField("end_station_id", StringType(), nullable=True),
            StructField("start_lat", DoubleType(), nullable=True),
            StructField("start_lng", DoubleType(), nullable=True),
            StructField("end_lat", DoubleType(), nullable=True),
            StructField("end_lng", DoubleType(), nullable=True),
            StructField("member_casual", StringType(), nullable=True),
        ]
    )


class DatabricksMedallionPipeline:
    """Executes Bronze -> Silver -> Gold Medallion transformations in PySpark & Delta Lake."""

    def __init__(self, spark: SparkSession, config: PipelineConfig) -> None:
        self.spark = spark
        self.config = config
        self.catalog = config.databricks_catalog
        self.bronze_table = f"{self.catalog}.{config.bronze_schema}.raw_trips"
        self.silver_table = f"{self.catalog}.{config.silver_schema}.clean_trips"
        self.gold_categorical_table = f"{self.catalog}.{config.gold_schema}.mart_categorical_usage"
        self.gold_temporal_table = f"{self.catalog}.{config.gold_schema}.mart_temporal_mobility"
        self.gold_station_table = f"{self.catalog}.{config.gold_schema}.dim_stations"

    def run_bronze_ingestion(self, s3_source_uri: str | None = None) -> DataFrame:
        """Read raw Citi Bike CSVs from S3 Data Lake and write append-only Bronze Delta table."""
        from pyspark.sql import functions as F

        source_uri = (
            s3_source_uri
            or f"s3://{self.config.s3_bucket_name}/{self.config.s3_raw_prefix}/*/*.csv"
        )
        logger.info("Ingesting Bronze Delta table %s from %s", self.bronze_table, source_uri)

        raw_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .schema(get_citibike_spark_schema())
            .load(source_uri)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )

        (
            raw_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.bronze_table)
        )
        return raw_df

    def transform_bronze_to_silver_df(self, bronze_df: DataFrame) -> DataFrame:
        """Clean, deduplicate, validate trip durations, and enrich with spatial/temporal features."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        dedup_window = Window.partitionBy("ride_id").orderBy(F.col("_ingested_at").desc())

        cleaned = (
            bronze_df.filter(
                F.col("ride_id").isNotNull()
                & F.col("started_at").isNotNull()
                & F.col("ended_at").isNotNull()
            )
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .withColumn("started_at_ts", F.to_timestamp("started_at"))
            .withColumn("ended_at_ts", F.to_timestamp("ended_at"))
            .withColumn(
                "trip_duration_seconds",
                F.unix_timestamp("ended_at_ts") - F.unix_timestamp("started_at_ts"),
            )
            .withColumn("trip_duration_minutes", F.round(F.col("trip_duration_seconds") / 60.0, 2))
            # Filter out invalid/cancelled trips (< 1 min) and stuck/stolen bikes (> 24 hours)
            .filter(
                (F.col("trip_duration_seconds") >= 60)
                & (F.col("trip_duration_seconds") <= 86400)
                & F.col("start_lat").between(40.4, 41.0)
                & F.col("start_lng").between(-74.3, -73.6)
            )
            .withColumn("started_date", F.to_date("started_at_ts"))
            .withColumn("trip_year_month", F.date_format("started_at_ts", "yyyy-MM"))
            .withColumn("start_hour", F.hour("started_at_ts"))
            .withColumn("day_of_week_num", F.dayofweek("started_at_ts"))
            .withColumn("day_of_week_name", F.date_format("started_at_ts", "EEEE"))
            .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week_num").isin(1, 7), F.lit(True)).otherwise(F.lit(False)),
            )
            .withColumn(
                "time_of_day_bucket",
                F.when(F.col("start_hour").between(6, 9), F.lit("Morning Rush (06-09)"))
                .when(F.col("start_hour").between(10, 15), F.lit("Midday (10-15)"))
                .when(F.col("start_hour").between(16, 19), F.lit("Evening Rush (16-19)"))
                .otherwise(F.lit("Night / Off-Peak (20-05)")),
            )
            .withColumn(
                "rideable_type_clean",
                F.when(F.col("rideable_type") == "electric_bike", F.lit("Electric Bike"))
                .when(F.col("rideable_type") == "classic_bike", F.lit("Classic Bike"))
                .when(F.col("rideable_type") == "docked_bike", F.lit("Docked Bike"))
                .otherwise(F.initcap(F.regexp_replace("rideable_type", "_", " "))),
            )
            .withColumn(
                "rider_type",
                F.when(F.lower(F.col("member_casual")) == "member", F.lit("Annual Member")).otherwise(
                    F.lit("Casual Rider")
                ),
            )
            # Haversine great-circle distance in kilometers
            .withColumn(
                "trip_distance_km",
                F.round(
                    F.lit(2.0 * 6371.0)
                    * F.asin(
                        F.sqrt(
                            F.pow(
                                F.sin((F.radians("end_lat") - F.radians("start_lat")) / 2.0),
                                2,
                            )
                            + F.cos(F.radians("start_lat"))
                            * F.cos(F.radians("end_lat"))
                            * F.pow(
                                F.sin((F.radians("end_lng") - F.radians("start_lng")) / 2.0),
                                2,
                            )
                        )
                    ),
                    3,
                ),
            )
            .withColumn(
                "is_round_trip",
                F.when(
                    F.col("start_station_id").isNotNull()
                    & (F.col("start_station_id") == F.col("end_station_id")),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
        )
        return cleaned

    def run_silver_transformation(self) -> DataFrame:
        """Write partitioned Silver Delta table and apply Z-ORDER clustering optimization."""
        bronze_df = self.spark.table(self.bronze_table)
        silver_df = self.transform_bronze_to_silver_df(bronze_df)

        logger.info("Writing partitioned Silver Delta table %s", self.silver_table)
        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("trip_year_month")
            .saveAsTable(self.silver_table)
        )

        # Optimize layout using Delta Lake Z-ORDER for upstream analytical queries
        logger.info("Running OPTIMIZE ZORDER BY on %s", self.silver_table)
        self.spark.sql(
            f"OPTIMIZE {self.silver_table} ZORDER BY (start_station_id, started_date, member_casual)"
        )
        return silver_df

    def run_gold_marts(self) -> None:
        """Create Gold analytical marts optimized for BI dashboards."""
        from pyspark.sql import functions as F

        silver_df = self.spark.table(self.silver_table)

        # 1. Gold Categorical Usage Mart (Tile 1: Categorical Distribution)
        categorical_mart = (
            silver_df.groupBy(
                "trip_year_month",
                "rider_type",
                "rideable_type_clean",
                "time_of_day_bucket",
                "is_weekend",
            )
            .agg(
                F.count("ride_id").alias("total_trips"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
                F.round(F.avg("trip_distance_km"), 3).alias("avg_distance_km"),
                F.sum(F.when(F.col("is_round_trip"), 1).otherwise(0)).alias("round_trip_count"),
            )
        )
        (
            categorical_mart.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.gold_categorical_table)
        )

        # 2. Gold Temporal Mobility Mart (Tile 2: Temporal Distribution)
        temporal_mart = (
            silver_df.groupBy(
                "started_date",
                "trip_year_month",
                "start_hour",
                "day_of_week_name",
                "is_weekend",
                "rider_type",
                "rideable_type_clean",
            )
            .agg(
                F.count("ride_id").alias("total_trips"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
                F.round(F.avg("trip_distance_km"), 3).alias("avg_distance_km"),
            )
        )
        (
            temporal_mart.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("trip_year_month")
            .saveAsTable(self.gold_temporal_table)
        )
        self.spark.sql(f"OPTIMIZE {self.gold_temporal_table} ZORDER BY (started_date, rider_type)")

        # 3. Gold Station Dimension & Performance Mart
        station_mart = (
            silver_df.filter(F.col("start_station_id").isNotNull())
            .groupBy("start_station_id", "start_station_name")
            .agg(
                F.round(F.avg("start_lat"), 6).alias("station_lat"),
                F.round(F.avg("start_lng"), 6).alias("station_lng"),
                F.count("ride_id").alias("total_departures"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
                F.round(F.avg("trip_distance_km"), 3).alias("avg_distance_km"),
                F.round(
                    F.sum(F.when(F.col("rider_type") == "Annual Member", 1).otherwise(0))
                    * 100.0
                    / F.count("ride_id"),
                    1,
                ).alias("member_share_pct"),
            )
        )
        (
            station_mart.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.gold_station_table)
        )
