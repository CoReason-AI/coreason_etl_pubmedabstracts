# Copyright (c) 2025 CoReason, Inc.
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import argparse
import sys
import subprocess
from typing import List, Optional

import dlt
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


def run_dbt_transformations(project_dir: str = "dbt_pubmed") -> None:
    logger.info("Starting dbt transformations...")
    dbt_command = ["dbt", "build", "--profiles-dir", "."]

    try:
        logger.info(f"Running dbt build in {project_dir}...")
        subprocess.run(dbt_command, cwd=project_dir, check=True, text=True)
        logger.info("dbt transformations completed successfully.")

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt build failed with exit code {e.returncode}")
        raise e
    except FileNotFoundError:
        logger.error("dbt executable not found. Ensure dbt-postgres is installed.")
        raise


def _ensure_tables_exist(pipeline: dlt.Pipeline) -> None:
    """Ensures that required tables exist for dbt to run."""
    logger.info("Ensuring schema and tables exist for dbt...")
    
    # We update this to match the dlt resource name
    table_name = "pubmed_abstract_updates"
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {pipeline.dataset_name}.{table_name} (
        file_name text,
        ingestion_ts double precision,
        content_hash text,
        raw_data jsonb,
        _dlt_load_id text,
        _dlt_id text
    );
    """
    try:
        with pipeline.sql_client() as client:
            client.execute_sql(create_sql)
            logger.info(f"Verified existence of {table_name}.")
    except Exception as e:
        logger.warning(f"Failed to ensure table existence: {e}")


def _prepare_baseline_load(pipeline: dlt.Pipeline, resource_name: str) -> None:
    logger.info(f"Checking incremental state for resource '{resource_name}'...")
    try:
        source_state = pipeline.state.get("sources", {}).get("pubmed_source", {})
        resource_state = source_state.get("resources", {}).get(resource_name, {})
        
        if not resource_state:
             logger.info(f"No state found for {resource_name}. Proceeding with fresh load.")
    except Exception as e:
        logger.warning(f"State check failed: {e}. Proceeding.")


def run_pipeline(load_target: str, dry_run: bool = False) -> None:
    logger.info(f"Initializing pipeline with target: {load_target}")

    # 1. Pipeline configuration is handled via .dlt/config.toml
    # Ensure .dlt/config.toml contains: [pipeline] dataset_name = "bronze"
    pipeline = dlt.pipeline(
        pipeline_name="pubmed_abstracts",
        progress="log",
    )
    
    if dry_run:
        return

    source = pubmed_source()
    
    # 2. Use the default resource name explicitly
    resource_key = "pubmed_abstract_baseline"

    # Verify the resource exists
    if resource_key not in source.resources:
        logger.error(f"Could not find resource '{resource_key}'. Available: {source.resources.keys()}")
        sys.exit(1)

    # 3. Filter resources based on load target
    if load_target == "baseline":
        source = source.with_resources(resource_key)
    
    _prepare_baseline_load(pipeline, resource_key)

    # 4. Run the pipeline
    info = pipeline.run(source)
    logger.info(f"Pipeline run completed. Load Info: {info}")

    if info.has_failed_jobs:
        logger.error("Pipeline run reported failed jobs!")
        sys.exit(1)

    _ensure_tables_exist(pipeline)
    run_dbt_transformations(project_dir="dbt_pubmed")


@logger.catch
def main() -> None:
    args = get_args()
    try:
        run_pipeline(args.load, args.dry_run)
    except Exception:
        logger.exception("Pipeline execution failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
