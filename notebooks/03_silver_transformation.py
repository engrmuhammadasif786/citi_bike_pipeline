"""Databricks Workflow Task 3: Transform Bronze to Partitioned & Z-Ordered Silver Delta table."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.transformations.medallion_pyspark import DatabricksMedallionPipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DATABRICKS_CATALOG", "citi_bike_catalog"))
    args, _ = parser.parse_known_args()

    os.environ["DATABRICKS_CATALOG"] = args.catalog

    spark = SparkSession.builder.appName("03_Silver_Transformation").getOrCreate()
    config = PipelineConfig()
    pipeline = DatabricksMedallionPipeline(spark, config)
    pipeline.run_silver_transformation()


if __name__ == "__main__":
    main()
