"""Databricks Workflow Task 2: Load raw CSV files from AWS S3 into Unity Catalog Bronze Delta table."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.transformations.medallion_pyspark import DatabricksMedallionPipeline


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
