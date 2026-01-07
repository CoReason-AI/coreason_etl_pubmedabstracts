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
from typing import Any, Dict, Iterator, List

import dlt
from dlt.sources import DltResource
from dlt.sources.filesystem import FileItem, filesystem
from loguru import logger

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


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


@dlt.transformer(name="pubmed_xml_parser")  # type: ignore[misc]
def pubmed_xml_parser(file_items: List[FileItem]) -> Iterator[Dict[str, Any]]:
    """
    Transformer that takes a list of FileItems (yielded by dlt.sources.filesystem),
    opens each file, parses the XML, and yields wrapped records.
    """
    for file_item in file_items:
        file_name = file_item["file_name"]
        logger.info(f"Processing file: {file_name}")

        try:
            # dlt's FileItemDict has a .open() method that returns a file-like object.
            # It wraps fs_client.open(...)
            with file_item.open() as f:
                for record in parse_pubmed_xml(f):
                    yield _wrap_record(record, file_name)
        except Exception as e:
            logger.error(f"Failed to process file {file_name}: {e}")
            raise e


def _create_pubmed_resource(base_url: str, subfolder: str, resource_name: str) -> DltResource:
    """
    Helper to create a dlt resource for a specific PubMed subfolder (baseline or updates).
    """
    # Ensure base_url ends with / and subfolder does not start with / to avoid double slashes
    full_url = base_url.rstrip("/") + "/" + subfolder.strip("/") + "/"

    return (
        filesystem(
            bucket_url=full_url,
            file_glob="*.xml.gz",
            incremental=dlt.sources.incremental("file_name"),  # noqa: B008
        )
        | pubmed_xml_parser
    ).with_name(resource_name)


@dlt.source  # type: ignore[misc]
def pubmed_source() -> Iterator[DltResource]:
    """
    The main DLT source for PubMed using native filesystem logic.
    """
    # We rely on dlt config (secrets.toml) to provide bucket_url.
    # [sources.pubmed.filesystem] -> bucket_url is the base.
    base_url = dlt.config.get("sources.pubmed.filesystem.bucket_url", "ftp://ftp.ncbi.nlm.nih.gov/pubmed/")

    # 1. Baseline
    yield _create_pubmed_resource(base_url, "baseline", "pubmed_baseline")

    # 2. Updates
    yield _create_pubmed_resource(base_url, "updatefiles", "pubmed_updates")
