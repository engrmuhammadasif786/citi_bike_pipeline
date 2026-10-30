"""Centralized environment configuration for the Citi Bike Lakehouse Pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration resolved exclusively from environment variables with safe defaults."""

    pipeline_env: str = field(
        default_factory=lambda: os.getenv("PIPELINE_ENV", "local").strip().lower()
    )
    year_months: list[str] = field(
        default_factory=lambda: [
            ym.strip()
            for ym in os.getenv("CITIBIKE_YEAR_MONTHS", "202401,202402,202403").split(",")
            if ym.strip()
        ]
    )
    dataset_prefix: str = field(
        default_factory=lambda: os.getenv("CITIBIKE_DATASET_PREFIX", "JC-").strip()
    )
    source_base_url: str = field(
        default_factory=lambda: os.getenv(
            "CITIBIKE_SOURCE_BASE_URL", "https://s3.amazonaws.com/tripdata"
        ).rstrip("/")
    )

    # AWS S3 Data Lake settings
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    s3_bucket_name: str = field(
        default_factory=lambda: os.getenv("S3_BUCKET_NAME", "citi-bike-lakehouse-raw-dev")
    )
    s3_raw_prefix: str = field(
        default_factory=lambda: os.getenv("S3_RAW_PREFIX", "raw/citibike/tripdata").strip("/")
    )

    # Databricks Unity Catalog & SQL Warehouse settings
    databricks_host: str = field(default_factory=lambda: os.getenv("DATABRICKS_HOST", ""))
    databricks_token: str = field(default_factory=lambda: os.getenv("DATABRICKS_TOKEN", ""))
    databricks_http_path: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_HTTP_PATH", "")
    )
    databricks_catalog: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_CATALOG", "citi_bike_catalog")
    )
    bronze_schema: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_BRONZE_SCHEMA", "bronze")
    )
    silver_schema: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_SILVER_SCHEMA", "silver")
    )
    gold_schema: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_GOLD_SCHEMA", "gold")
    )

    # Local storage paths
    local_data_dir: Path = field(
        default_factory=lambda: Path(os.getenv("LOCAL_DATA_DIR", "./data")).resolve()
    )

    @property
    def raw_dir(self) -> Path:
        return self.local_data_dir / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self.local_data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.local_data_dir / "silver"

    @property
    def gold_dir(self) -> Path:
        return self.local_data_dir / "gold"

    @property
    def duckdb_path(self) -> Path:
        return self.local_data_dir / "citi_bike_lakehouse.duckdb"

    def ensure_local_dirs(self) -> None:
        """Create local Medallion directories if they do not exist."""
        for directory in (self.raw_dir, self.bronze_dir, self.silver_dir, self.gold_dir):
            directory.mkdir(parents=True, exist_ok=True)
