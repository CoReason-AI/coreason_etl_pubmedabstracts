# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import subprocess
import os
import dlt
from coreason_etl_pubmedabstracts.utils.logger import logger

def run_deduplication_sweep(pipeline: dlt.Pipeline) -> None:
    """
    Perform a deduplication sweep by invoking the dbt macro `deduplicate_updates`.

    This ensures that the Baseline serves as the single source of truth for
    historical data and prevents duplication when a new Baseline is loaded.

    Args:
        pipeline: The active dlt Pipeline instance (used to infer config if needed,
                  but here we rely on dbt profile).
    """
    logger.info("Starting Deduplication Sweep via dbt...")

    # We assume the dbt project is in "dbt_pubmed" directory relative to CWD
    dbt_project_dir = "dbt_pubmed"

    if not os.path.isdir(dbt_project_dir):
        logger.error(f"dbt project directory not found at {dbt_project_dir}")
        raise FileNotFoundError(f"dbt project directory not found at {dbt_project_dir}")

    # Ensure profile matches what dlt might have set up, or rely on standard profile.
    # We will use the 'coreason_etl_pubmedabstracts' profile as defined in dbt_project.yml

    cmd = [
        "dbt",
        "run-operation",
        "deduplicate_updates",
        "--project-dir",
        dbt_project_dir,
        "--profiles-dir",
        dbt_project_dir,  # Assuming profiles.yml is in the project dir for portability
    ]

    try:
        # Run dbt command
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"dbt operation output:\n{result.stdout}")
        logger.info("Deduplication Sweep completed successfully via dbt.")

    except subprocess.CalledProcessError as e:
        logger.error(f"Deduplication Sweep failed via dbt.\nStdout: {e.stdout}\nStderr: {e.stderr}")
        raise e
