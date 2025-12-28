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
import sys
from typing import List, Optional

import dlt
from loguru import logger

from coreason_etl_pubmedabstracts.pipelines.deduplication import run_deduplication_sweep
from coreason_etl_pubmedabstracts.pipelines.pubmed_pipeline import pubmed_source


def get_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coreason ETL PubMed Abstracts Pipeline"
    )
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
    # We pass the source function and select specific resources
    info = pipeline.run(pubmed_source(), table_name=resources_to_run)
    logger.info(f"Pipeline run completed. Load Info: {info}")

    # 2. Check for success (basic check)
    # dlt raises exceptions on severe errors, but we can check load_info too
    # If using dlt < 0.4, checking logic might vary, but assuming recent dlt.
    if info.has_failed_jobs:
        logger.error("Pipeline run reported failed jobs!")
        sys.exit(1)

    # 3. Conditional Deduplication Sweep
    # Only run if we loaded the baseline (and it was successful)
    if "pubmed_baseline" in resources_to_run:
        logger.info("Baseline loaded. Triggering Deduplication Sweep...")
        run_deduplication_sweep(pipeline)
        logger.info("Deduplication Sweep finished.")


def main() -> None:
    args = get_args()
    try:
        run_pipeline(args.load, args.dry_run)
    except Exception as e:
        logger.exception("Pipeline execution failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
