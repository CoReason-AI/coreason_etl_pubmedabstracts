# Copyright (c) 2025 CoReason, Inc.
# ... [License] ...

import hashlib
import json
import time
import gzip
import tempfile
import os
import re  # Added for parsing HTML links
import requests
from typing import Any, Dict, Iterator

import dlt
from dlt.sources import DltResource
from loguru import logger

from coreason_etl_pubmedabstracts.pipelines.xml_utils import parse_pubmed_xml


# Transformer: Downloads and Parses the file
@dlt.transformer(
    name="pubmed_xml_parser", 
    write_disposition="append",
    columns={"raw_data": {"data_type": "json"}}
)
def download_and_parse(file_info: Dict[str, str]) -> Iterator[Dict[str, Any]]:
    """
    Downloads a file from a URL to a temp file, decompresses it, and parses it.
    """
    url = file_info["file_url"]
    file_name = file_info["file_name"]
    
    logger.info(f"Starting download for {file_name} from {url}")
    
    # 1. Download to a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
        try:
            # Stream download to avoid memory issues
            with requests.get(url, stream=True, timeout=600) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp.write(chunk)
            tmp.close() # Close handle to allow reading

            # 2. Parse the downloaded file
            logger.info(f"Download complete. Parsing {file_name}...")
            count = 0
            with gzip.open(tmp_path, "rb") as f:
                for record in parse_pubmed_xml(f):
                    count += 1
                    
                    raw_json = json.dumps(record, sort_keys=True)
                    content_hash = hashlib.md5(raw_json.encode("utf-8")).hexdigest()
                    
                    yield {
                        "file_name": file_name,
                        "ingestion_ts": time.time(),
                        "content_hash": content_hash,
                        "raw_data": record, 
                    }
            
            logger.info(f"Successfully processed {count} records from {file_name}")

        except Exception as e:
            logger.error(f"Failed to process {file_name}: {e}")
            raise e
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


@dlt.source
def pubmed_source() -> Iterator[DltResource]:
    """
    Dynamically scrapes the NCBI directory to find all baseline files
    and yields them for processing.
    """
    base_url = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
    logger.info(f"Fetching file list from {base_url}...")

    try:
        # Fetch the directory listing
        response = requests.get(base_url, timeout=60)
        response.raise_for_status()
        
        # Use regex to find all links ending in .xml.gz
        # HTML links usually look like: <a href="pubmed25n1270.xml.gz">
        pattern = r'href="(pubmed\d+n\d+\.xml\.gz)"'
        found_files = re.findall(pattern, response.text)
        
        # Deduplicate and sort the file list
        unique_files = sorted(list(set(found_files)))
        
        if not unique_files:
            logger.warning(f"No .xml.gz files found at {base_url}")
            return

        logger.info(f"Found {len(unique_files)} files to process.")

        # Construct the file info objects
        files_to_process = [
            {
                "file_name": filename,
                "file_url": f"{base_url}{filename}"
            }
            for filename in unique_files
        ]

        # Create a resource for the file list
        baseline_files = dlt.resource(files_to_process, name="baseline_files")
        
        # Pipe it into the downloader/parser
        yield (baseline_files | download_and_parse).with_name("pubmed_abstract_baseline")

    except Exception as e:
        logger.error(f"Failed to initialize pubmed source: {e}")
        raise e
