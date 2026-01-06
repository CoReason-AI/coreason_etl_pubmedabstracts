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
from typing import Any, Dict, Iterator, List, Optional

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


@dlt.transformer(name="pubmed_xml_parser")
def pubmed_xml_parser(file_items: List[FileItem]) -> Iterator[Dict[str, Any]]:
    """
    Transformer that takes a list of FileItems (yielded by dlt.sources.filesystem),
    opens each file, parses the XML, and yields wrapped records.
    """
    for file_item in file_items:
        file_name = file_item["file_name"]
        logger.info(f"Processing file: {file_name}")

        # Open the file using the helper method provided by FileItem
        # This handles the fsspec integration and compression
        # We need to use 'open' method of the item or the fs_client.
        # FileItem is a dict-like object with an 'open' method?
        # Checking dlt docs/source: FileItemDict has a .open() method?
        # Actually dlt source code: file_dict = FileItemDict(file_model, fs_client)
        # FileItemDict implementation likely has open().

        try:
            # We can use the 'open' method on the file_item if available,
            # or use the fs_client inside it if exposed.
            # dlt's FileItemDict has a .open() method that returns a file-like object.
            # It wraps fs_client.open(...)
            with file_item.open() as f:
                for record in parse_pubmed_xml(f):
                    yield _wrap_record(record, file_name)
        except Exception as e:
            logger.error(f"Failed to process file {file_name}: {e}")
            raise e


@dlt.source
def pubmed_source() -> Iterator[DltResource]:
    """
    The main DLT source for PubMed using native filesystem logic.
    """

    # We define the incremental config for file_name
    # dlt filesystem source uses 'file_name' field in FileItem.

    # Resource: Baseline
    # We rely on dlt config (secrets.toml) to provide bucket_url.
    # [sources.pubmed.filesystem] -> bucket_url is the base.
    # But here we need specific subdirectories.
    # We can override bucket_url in the call.

    # We'll use dlt.config.value to get the base url if we wanted, but we can also just rely on
    # specific structure.
    # To keep it standard, we'll assume the user configures the base url in secrets,
    # and we append the path. But filesystem source takes a full bucket_url.

    # Let's check if we can retrieve the configured base url.
    # Or we can just use the provided default in the function signature if we kept it.
    # But we want to use dlt config.

    # Better approach: Define two resources using filesystem factory.

    # We need to construct the URL. We can ask dlt to inject the base URL.
    # But filesystem() expects bucket_url as an argument.

    # Let's use dlt.config to get the base URL.
    # We assume [sources.pubmed] section exists.

    base_url = dlt.config.get("sources.pubmed.filesystem.bucket_url", "ftp://ftp.ncbi.nlm.nih.gov/pubmed/")

    # Ensure base_url ends with /
    base_url = base_url.rstrip("/") + "/"

    # 1. Baseline
    yield (
        filesystem(
            bucket_url=base_url + "baseline/",
            file_glob="*.xml.gz",
            incremental=dlt.sources.incremental("file_name"),
        )
        | pubmed_xml_parser
    ).with_name("pubmed_baseline")

    # 2. Updates
    yield (
        filesystem(
            bucket_url=base_url + "updatefiles/",
            file_glob="*.xml.gz",
            incremental=dlt.sources.incremental("file_name"),
        )
        | pubmed_xml_parser
    ).with_name("pubmed_updates")
