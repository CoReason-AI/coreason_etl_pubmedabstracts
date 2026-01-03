# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import argparse
import subprocess
import sys
from typing import List, Optional

import dlt

from coreason_etl_pubmedabstracts.pipelines.deduplication import run_deduplication_sweep
from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import pubmed_source
from coreason_etl_pubmedabstracts.utils.logger import logger


def get_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coreason ETL PubMed Abstracts Pipeline")
    parser.add_argument(
        "--load",
        choices=["baseline", "updates", "all"],
        default="all",
        help="Which dataset to load (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="If set, initializes the pipeline but does not run ingestion.",
    )
    return parser.parse_args(args)


def run_dbt_transformations(project_dir: str = "dbt_pubmed") -> None:
    """
    Execute dbt build to transform loaded data.
    """
    logger.info("Starting dbt transformations...")
    try:
        # We assume 'dbt' is in the path (installed via poetry)
        subprocess.run(
            ["dbt", "build", "--project-dir", project_dir],
            check=True,
            capture_output=False,  # Let dbt output stream to stdout/stderr
        )
        logger.info("dbt transformations completed successfully.")
    except FileNotFoundError as e:
        logger.error("dbt executable not found. Ensure dbt is installed and in the PATH.")
        raise e
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt transformations failed with exit code {e.returncode}")
        raise e


def run_pipeline(load_target: str, dry_run: bool = False) -> None:
    """
    Orchestrate the ETL pipeline.

    Args:
        load_target: 'baseline', 'updates', or 'all'.
        dry_run: If True, skip actual execution.
    """
    logger.info(f"Initializing pipeline with target: {load_target}")

    # Initialize the dlt pipeline
    # dataset_name should match what is expected by dbt (e.g. 'pubmed')
    pipeline = dlt.pipeline(
        pipeline_name="pubmed_abstracts",
        destination="postgres",
        dataset_name="pubmed",
        progress="log",
    )

    if dry_run:
        logger.info("Dry run enabled. Exiting before execution.")
        return

    # Determine which resources to run
    resources_to_run = []
    if load_target in ("baseline", "all"):
        resources_to_run.append("pubmed_baseline")
    if load_target in ("updates", "all"):
        resources_to_run.append("pubmed_updates")

    if not resources_to_run:
        logger.warning("No resources selected to run.")
        return

    logger.info(f"Running resources: {resources_to_run}")

    # 1. Run the Pipeline
    source = pubmed_source()
    # Filter resources using .with_resources()
    source = source.with_resources(*resources_to_run)

    info = pipeline.run(source)
    logger.info(f"Pipeline run completed. Load Info: {info}")

    # 2. Check for success (basic check)
    if info.has_failed_jobs:
        logger.error("Pipeline run reported failed jobs!")
        sys.exit(1)

    # 3. Conditional Deduplication Sweep
    # Only run if we loaded the baseline (and it was successful)
    if "pubmed_baseline" in resources_to_run:
        logger.info("Baseline loaded. Triggering Deduplication Sweep...")
        run_deduplication_sweep(pipeline)
        logger.info("Deduplication Sweep finished.")

    # 4. Run dbt transformations
    # This runs regardless of load target, as updates also need transformation (Silver/Gold)
    run_dbt_transformations()


@logger.catch  # type: ignore
def main() -> None:
    args = get_args()
    try:
        run_pipeline(args.load, args.dry_run)
    except Exception:
        logger.exception("Pipeline execution failed")
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
