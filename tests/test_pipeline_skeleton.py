# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import dlt

from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import pubmed_source


def test_dependencies_installed() -> None:
    """Check that key dependencies are importable."""
    import dlt
    import lxml
    import xmltodict

    assert lxml is not None
    assert xmltodict is not None
    assert dlt is not None


def test_pipeline_creation() -> None:
    """Test that we can create the pipeline and inspect the source."""
    pipeline = dlt.pipeline(
        pipeline_name="test_pubmed_pipeline",
        destination="duckdb",
        dataset_name="pubmed_test",
    )
    assert pipeline is not None

    source = pubmed_source()
    assert source is not None

    # Verify the source has the expected resources
    resources = list(source.resources.keys())
    assert "pubmed_baseline" in resources


def test_pipeline_run() -> None:
    """Run the skeleton pipeline to verify end-to-end execution (no-op)."""
    pipeline = dlt.pipeline(
        pipeline_name="test_pubmed_run",
        destination="duckdb",
        dataset_name="pubmed_test_run",
    )

    source = pubmed_source()
    info = pipeline.run(source)

    assert info.has_failed_jobs is False
    # Check that it loaded something
    assert info.pipeline.pipeline_name == "test_pubmed_run"
