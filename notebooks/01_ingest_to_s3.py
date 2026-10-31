"""Databricks Workflow Task 1: Ingest monthly Citi Bike tripdata archives to AWS S3 Data Lake."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.ingestion.citibike_ingestor import CitiBikeIngestor


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.getenv("S3_BUCKET_NAME", "citi-bike-lakehouse-raw-dev"))
    parser.add_argument("--year-months", default=os.getenv("CITIBIKE_YEAR_MONTHS", "202401,202402,202403"))
    args, _ = parser.parse_known_args()

    os.environ["S3_BUCKET_NAME"] = args.s3_bucket
    os.environ["CITIBIKE_YEAR_MONTHS"] = args.year_months

    config = PipelineConfig()
    ingestor = CitiBikeIngestor(config)
    ingestor.ingest_all(upload_s3=True)


if __name__ == "__main__":
    main()
