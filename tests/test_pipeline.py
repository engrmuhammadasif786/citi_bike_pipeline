"""Unit and integration test suite for the NYC Citi Bike Medallion Lakehouse Pipeline."""

from __future__ import annotations

import csv
from pathlib import Path
from unittest.mock import MagicMock

from citi_bike_pipeline.config import PipelineConfig
from citi_bike_pipeline.ingestion.citibike_ingestor import (
    EXPECTED_COLUMNS,
    CitiBikeIngestor,
    candidate_archive_urls,
    generate_fallback_csv,
)
from citi_bike_pipeline.transformations.medallion_local import LocalMedallionPipeline


def make_test_config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        pipeline_env="local",
        year_months=["202401", "202402"],
        dataset_prefix="JC-",
        local_data_dir=tmp_path / "data",
    )


def test_candidate_archive_urls() -> None:
    urls = candidate_archive_urls("https://s3.amazonaws.com/tripdata", "JC-", "202401")
    assert urls == [
        "https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.csv.zip",
        "https://s3.amazonaws.com/tripdata/JC-202401-citibike-tripdata.zip",
    ]


def test_ingestor_graceful_404_fallback(tmp_path: Path) -> None:
    """Verify that HTTP 404 errors on remote archives gracefully trigger fallback generation."""
    config = make_test_config(tmp_path)
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_session.get.return_value = mock_response

    ingestor = CitiBikeIngestor(config, session=mock_session)
    csv_path = ingestor.download_and_extract_month("202401", allow_fallback=True)

    assert csv_path.exists()
    assert csv_path.stat().st_size > 0
    with csv_path.open("r", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == EXPECTED_COLUMNS


def test_medallion_bronze_silver_gold_and_quality(tmp_path: Path) -> None:
    """Verify end-to-end Bronze -> Silver -> Gold transformations, deduplication, and quality checks."""
    config = make_test_config(tmp_path)
    config.ensure_local_dirs()

    # Generate valid sample data for 202401
    csv_path = config.raw_dir / "JC-202401-citibike-tripdata.csv"
    generate_fallback_csv("202401", csv_path, num_rows=600)

    # Append a duplicate ride_id and an invalid 10-second cancelled ride to verify Silver filtering
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Duplicate ride_id
        writer.writerow(
            [
                "20240100000000",
                "electric_bike",
                "2024-01-05 08:00:00",
                "2024-01-05 08:15:00",
                "Grove St PATH",
                "JC013",
                "Exchange Place",
                "JC014",
                "40.719586",
                "-74.043117",
                "40.716366",
                "-74.034344",
                "member",
            ]
        )
        # Invalid short duration (< 60s)
        writer.writerow(
            [
                "SHORT_INVALID_RIDE",
                "classic_bike",
                "2024-01-05 09:00:00",
                "2024-01-05 09:00:15",
                "Grove St PATH",
                "JC013",
                "Grove St PATH",
                "JC013",
                "40.719586",
                "-74.043117",
                "40.719586",
                "-74.043117",
                "casual",
            ]
        )

    pipeline = LocalMedallionPipeline(config)
    bronze_count = pipeline.run_bronze([csv_path])
    silver_count = pipeline.run_silver()
    gold_counts = pipeline.run_gold()
    quality_results = pipeline.run_quality_checks()

    assert bronze_count > silver_count
    assert silver_count == bronze_count - 2
    assert gold_counts["mart_categorical_usage"] > 0
    assert gold_counts["mart_temporal_mobility"] > 0
    assert gold_counts["dim_stations"] > 0
    assert all(r.passed for r in quality_results)

    # Verify partitioned Silver output exists by trip_year_month
    partition_dirs = list((config.silver_dir / "clean_trips_partitioned").glob("trip_year_month=*"))
    assert len(partition_dirs) == 1
    assert partition_dirs[0].name == "trip_year_month=2024-01"
