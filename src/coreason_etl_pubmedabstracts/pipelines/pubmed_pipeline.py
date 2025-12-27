# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

from typing import Iterator

import dlt
from dlt.sources import DltResource


@dlt.source
def pubmed_source() -> Iterator[DltResource]:
    """
    The main DLT source for PubMed.
    Currently a skeleton returning a dummy resource.
    """
    yield pubmed_baseline()


@dlt.resource(write_disposition="replace")
def pubmed_baseline() -> Iterator[dict[str, str]]:
    """
    A placeholder resource for PubMed Baseline.
    """
    yield {"message": "Hello from PubMed Baseline Skeleton"}
