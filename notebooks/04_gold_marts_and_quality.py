"""Databricks Workflow Task 4: Build Gold Analytical Marts and validate data quality."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.transformations.medallion_pyspark import DatabricksMedallionPipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "citi_bike_catalog"))
    args, _ = parser.parse_known_args()

    os.environ["DATABRICKS_CATALOG"] = args.catalog

    spark = SparkSession.builder.appName("04_Gold_Marts_And_Quality").getOrCreate()
    config = PipelineConfig()
    pipeline = DatabricksMedallionPipeline(spark, config)
    pipeline.run_gold_marts()


if __name__ == "__main__":
    main()
