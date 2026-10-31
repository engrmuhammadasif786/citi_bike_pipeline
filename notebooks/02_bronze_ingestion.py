"""Databricks Workflow Task 2: Load raw CSV files from AWS S3 into Unity Catalog Bronze Delta table."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

project_root = Path.cwd().resolve()
while project_root != project_root.parent and not (
    project_root / "src" / "citi_bike_pipeline"
).is_dir():
    project_root = project_root.parent

package_source = project_root / "src"
if not (package_source / "citi_bike_pipeline").is_dir():
    raise ModuleNotFoundError(f"Cannot locate src/citi_bike_pipeline from {Path.cwd()}")
sys.path.insert(0, str(package_source))

from citi_bike_pipeline.config import PipelineConfig  # noqa: E402
from citi_bike_pipeline.transformations.medallion_pyspark import DatabricksMedallionPipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "citi_bike_catalog"))
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET_NAME", "citi-bike-lakehouse-raw-dev"))
    args, _ = parser.parse_known_args()

    os.environ["DATABRICKS_CATALOG"] = args.catalog
    os.environ["S3_BUCKET_NAME"] = args.s3_bucket

    spark = SparkSession.builder.appName("02_Bronze_Ingestion").getOrCreate()
    config = PipelineConfig()
    pipeline = DatabricksMedallionPipeline(spark, config)
    pipeline.run_bronze_ingestion()


if __name__ == "__main__":
    main()
