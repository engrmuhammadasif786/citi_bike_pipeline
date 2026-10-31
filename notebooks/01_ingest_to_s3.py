"""Databricks Workflow Task 1: Ingest monthly Citi Bike tripdata archives to AWS S3 Data Lake."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

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
from citi_bike_pipeline.ingestion.citibike_ingestor import CitiBikeIngestor  # noqa: E402


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
