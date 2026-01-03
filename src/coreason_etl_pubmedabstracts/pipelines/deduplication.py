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

from coreason_etl_pubmedabstracts.utils.logger import logger


def run_deduplication_sweep(pipeline: dlt.Pipeline) -> None:
    """
    Perform a deduplication sweep to remove records from the Updates table
    that are present in the Baseline table.

    This ensures that the Baseline serves as the single source of truth for
    historical data and prevents duplication when a new Baseline is loaded.

    Args:
        pipeline: The active dlt Pipeline instance connected to the destination.
    """
    logger.info("Starting Deduplication Sweep: removing overlapping updates...")

    dataset_name = pipeline.dataset_name
    baseline_table = f"{dataset_name}.bronze_pubmed_baseline"
    updates_table = f"{dataset_name}.bronze_pubmed_updates"

    # Robust extraction logic for PMID
    # Handles both string (e.g., "123") and object (e.g., {"#text": "123"}) variants
    # Note: We must alias the tables in the query to use them in the CASE expression correctly
    def get_pmid_extract(table_alias: str) -> str:
        return f"""
        CASE
            WHEN jsonb_typeof({table_alias}.raw_data -> 'MedlineCitation' -> 'PMID' -> 0) = 'string'
            THEN {table_alias}.raw_data -> 'MedlineCitation' -> 'PMID' ->> 0
            ELSE {table_alias}.raw_data -> 'MedlineCitation' -> 'PMID' -> 0 ->> '#text'
        END
        """

    # Using DELETE ... USING for better performance on large datasets compared to IN (...)
    dedup_sql = f"""
    DELETE FROM {updates_table} AS u
    USING {baseline_table} AS b
    WHERE {get_pmid_extract("u")} = {get_pmid_extract("b")};
    """

    with pipeline.sql_client() as client:
        try:
            client.execute_sql(dedup_sql)
            logger.info("Deduplication Sweep completed successfully.")
        except Exception as e:
            logger.error(f"Deduplication Sweep failed: {e}")
            raise e
