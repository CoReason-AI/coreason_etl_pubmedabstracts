# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

import hashlib
import json
import time
from typing import Any, Dict, Iterator, Optional

import dlt
from dlt.sources import DltResource
from loguru import logger

from coreason_etl_pubmedabstracts.pipelines.ftp_utils import (
    list_remote_files,
    open_remote_file,
)
from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


@dlt.source  # type: ignore[misc]
def pubmed_source(host: str = "ftp.ncbi.nlm.nih.gov") -> Iterator[DltResource]:
    """
    The main DLT source for PubMed.
    """
    yield pubmed_baseline(host=host)
    yield pubmed_updates(host=host)


def _wrap_record(record: Dict[str, Any], file_name: str) -> Dict[str, Any]:
    """
    Wrap the record to match the Bronze schema requirements.
    Target Table: bronze_pubmed_raw
    Columns: file_name, ingestion_ts, content_hash, raw_data (JSONB)
    """
    # Serialize to JSON string for hashing and storage
    # Using sort_keys=True for deterministic hashing
    raw_json = json.dumps(record, sort_keys=True)
    content_hash = hashlib.md5(raw_json.encode("utf-8")).hexdigest()

    return {
        "file_name": file_name,
        "ingestion_ts": time.time(),
        "content_hash": content_hash,
        "raw_data": record,  # dlt handles JSON types
    }


@dlt.resource(write_disposition="append", table_name="bronze_pubmed_baseline")  # type: ignore[misc]
def pubmed_baseline(
    host: str = "ftp.ncbi.nlm.nih.gov",
    last_file: Optional[dlt.sources.incremental[str]] = dlt.sources.incremental("file_name"),  # noqa: B008
) -> Iterator[Dict[str, Any]]:
    """
    Resource for PubMed Baseline.
    Iterates over all baseline files, parses them, and yields records.
    Uses incremental loading to allow resuming from the last processed file.

    Strategy: "Resumable Replace"
    - The write_disposition is 'append' to enable 'incremental' tracking (dlt doesn't support incremental on 'replace').
    - To satisfy the "Annual Reload" (Replace) requirement, the orchestration layer (main.py)
      detects if this is a Fresh Run (no state) and explicitly TRUNCATES the table before loading.
    """
    # 1. List files
    files = list_remote_files(host, "/pubmed/baseline/")

    # Explicitly sort to ensure safety even if list_remote_files (or FTP) returns unsorted
    files = sorted(files)

    # Filter files based on incremental state
    start_value = last_file.last_value if last_file else None

    # 2. Iterate and process
    for file_path in files:
        file_name = file_path.split("/")[-1]

        # Skip if file_name is <= last processed file
        if start_value and file_name <= start_value:
            continue

        logger.info(f"Processing Baseline file: {file_path}")

        # 3. Open stream (decompression handled by open_remote_file/fsspec)
        with open_remote_file(host, file_path) as f:
            # 4. Parse XML
            for record in parse_pubmed_xml(f):
                yield _wrap_record(record, file_name)


@dlt.resource(write_disposition="append", table_name="bronze_pubmed_updates")  # type: ignore[misc]
def pubmed_updates(
    host: str = "ftp.ncbi.nlm.nih.gov",
    last_file: Optional[dlt.sources.incremental[str]] = dlt.sources.incremental("file_name"),  # noqa: B008
) -> Iterator[Dict[str, Any]]:
    """
    Resource for PubMed Updates.
    Iterates over all update files, parses them, and yields records.
    Uses incremental loading to skip already processed files.
    """
    files = list_remote_files(host, "/pubmed/updatefiles/")

    # Explicitly sort to ensure safety
    files = sorted(files)

    # Filter files based on incremental state
    # last_file.last_value is the file_name (e.g., pubmed24n1001.xml.gz)
    # files are full paths (e.g., /pubmed/updatefiles/pubmed24n1001.xml.gz)

    start_value = last_file.last_value if last_file else None

    for file_path in files:
        file_name = file_path.split("/")[-1]

        # Skip if file_name is <= last processed file
        # Alphanumeric comparison works for standard pubmed naming (pubmedYYnXXXX)
        if start_value and file_name <= start_value:
            continue

        logger.info(f"Processing Update file: {file_path}")

        with open_remote_file(host, file_path) as f:
            for record in parse_pubmed_xml(f):
                yield _wrap_record(record, file_name)
