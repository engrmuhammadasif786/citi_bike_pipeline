"""Command-line interface for running the NYC Citi Bike Medallion Lakehouse Pipeline."""

from __future__ import annotations

import argparse
import logging
import sys

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.ingestion.citibike_ingestor import CitiBikeIngestor
from citi_bike_pipeline.transformations.medallion_local import LocalMedallionPipeline


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )


def run_ingest(config: PipelineConfig) -> None:
    ingestor = CitiBikeIngestor(config)
    files = ingestor.ingest_all()
    logging.info("Completed ingestion of %d monthly archive(s).", len(files))


def run_transform(config: PipelineConfig) -> None:
    if config.pipeline_env == "databricks":
        from pyspark.sql import SparkSession

        from citi_bike_pipeline.transformations.medallion_pyspark import (
            DatabricksMedallionPipeline,
        )

        spark = SparkSession.builder.appName("CitiBikeMedallionPipeline").getOrCreate()
        dbx_pipeline = DatabricksMedallionPipeline(spark, config)
        dbx_pipeline.run_bronze_ingestion()
        dbx_pipeline.run_silver_transformation()
        dbx_pipeline.run_gold_marts()
    else:
        local_pipeline = LocalMedallionPipeline(config)
        local_pipeline.run_bronze()
        local_pipeline.run_silver()
        local_pipeline.run_gold()


def run_quality(config: PipelineConfig) -> None:
    local_pipeline = LocalMedallionPipeline(config)
    results = local_pipeline.run_quality_checks()
    for check in results:
        logging.info("[PASS] %s -> %s", check.check_name, check.details)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(
        description="NYC Citi Bike Medallion Lakehouse Pipeline CLI"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("ingest", help="Download Citi Bike archives and land to Data Lake")
    subparsers.add_parser("transform", help="Execute Bronze -> Silver -> Gold transformations")
    subparsers.add_parser("quality", help="Run data quality assertions")
    subparsers.add_parser("run-all", help="Execute full end-to-end pipeline")

    args = parser.parse_args(argv)
    config = PipelineConfig()

    if args.command == "ingest":
        run_ingest(config)
    elif args.command == "transform":
        run_transform(config)
    elif args.command == "quality":
        run_quality(config)
    elif args.command == "run-all":
        run_ingest(config)
        run_transform(config)
        if config.pipeline_env != "databricks":
            run_quality(config)

    return 0


if __name__ == "__main__":
    sys.exit(main())
