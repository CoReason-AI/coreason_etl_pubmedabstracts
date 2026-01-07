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
from dlt.helpers.dbt import create_runner
from dlt.sources import DltSource

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


def run_dbt_transformations(pipeline: dlt.Pipeline, project_dir: str = "dbt_pubmed") -> None:
    """
    Execute dbt build to transform loaded data using dlt's built-in dbt runner.
    """
    logger.info("Starting dbt transformations...")
    try:
        # Get credentials from the pipeline configuration
        with pipeline.destination_client() as client:
            # Create dlt's dbt runner
            # venv=None uses current environment (where dbt-postgres is installed)
            runner = create_runner(
                venv=None,
                credentials=client.config,
                working_dir=".",  # Current directory as base
                package_location=project_dir,  # Path to dbt project
            )

            # Use _run_dbt_command to execute 'dbt build'.
            # 'dbt build' is preferred over 'run' + 'test' as it runs models, tests,
            # seeds, and snapshots in DAG order, ensuring correctness and handling dependencies.
            # While _run_dbt_command is protected, it's the only way to invoke 'build'
            # via the current DBTPackageRunner API, and essential for the 'snapshot' requirement.
            logger.info("Running dbt build...")
            runner._run_dbt_command("build", cmd_params=["--fail-fast"])

        logger.info("dbt transformations completed successfully.")

    except Exception as e:
        logger.error(f"dbt transformations failed: {e}")
        raise e


def _prepare_baseline_load(pipeline: dlt.Pipeline, source: DltSource) -> None:
    """
    Handles "Resumable Replace" logic for the baseline load.
    Checks if incremental state exists for 'pubmed_baseline'.
    If not (fresh run), truncates the target table to ensure a clean start.
    """
    logger.info("Checking incremental state for pubmed_baseline...")
    try:
        # dlt state logic: check if 'pubmed_baseline' has processed files
        # The structure is: sources -> source_name -> resources -> resource_name -> incremental -> param
        state = pipeline.state
        source_state = state.get("sources", {}).get(source.name, {})
        resource_state = source_state.get("resources", {}).get("pubmed_baseline", {})
        incremental_state = resource_state.get("incremental", {}).get("file_name", {})

        # If no last_value, it implies a fresh run (or state reset).
        if not incremental_state.get("last_value"):
            logger.info("No incremental state found (Fresh Run). Truncating 'bronze_pubmed_baseline'...")
            # Use fully qualified table name: dataset_name.table_name
            table_name = f"{pipeline.dataset_name}.bronze_pubmed_baseline"
            with pipeline.sql_client() as client:
                try:
                    client.execute_sql(f"TRUNCATE TABLE {table_name}")
                    logger.info(f"Successfully truncated {table_name}.")
                except Exception as e:
                    logger.warning(f"Could not truncate {table_name} (might not exist yet): {e}")
        else:
            logger.info(
                f"Incremental state found (Resuming from {incremental_state.get('last_value')}). Skipping truncate."
            )

    except Exception as e:
        logger.warning(f"State check/truncation failed: {e}. Proceeding with load.")


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

    # 1. Prepare Source
    source = pubmed_source()
    # Filter resources using .with_resources()
    source = source.with_resources(*resources_to_run)

    # 2. Resumable Replace Logic for Baseline
    if "pubmed_baseline" in resources_to_run:
        _prepare_baseline_load(pipeline, source)

    # 3. Run the Pipeline
    info = pipeline.run(source)
    logger.info(f"Pipeline run completed. Load Info: {info}")

    # 4. Check for success (basic check)
    if info.has_failed_jobs:
        logger.error("Pipeline run reported failed jobs!")
        sys.exit(1)

    # 5. Run dbt transformations
    # This runs regardless of load target, as updates also need transformation (Silver/Gold)
    run_dbt_transformations(pipeline)


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
