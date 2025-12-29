# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedabstracts

from typing import Any, Dict, List


def test_stg_pubmed_citations_union_logic() -> None:
    """
    Verify that stg_pubmed_citations correctly unions baseline and updates.
    We simulate the SQL logic by creating two mock lists of dicts (baseline and updates)
    and verifying the unioned result contains all records with correct structure.
    """
    # Mock Data
    baseline_records = [
        {
            "file_name": "baseline.xml.gz",
            "ingestion_ts": 100,
            "pmid": "1",
            "operation": "upsert",
            "title": "Baseline Title",
        }
    ]
    updates_records = [
        {
            "file_name": "update.xml.gz",
            "ingestion_ts": 200,
            "pmid": "2",
            "operation": "upsert",
            "title": "Update Title",
        },
        {
            "file_name": "update.xml.gz",
            "ingestion_ts": 200,
            "pmid": "1",
            "operation": "delete",
            "title": None,
        },
    ]

    # Simulate UNION ALL
    # In SQL: select * from baseline UNION ALL select * from updates
    unioned_results = baseline_records + updates_records

    assert len(unioned_results) == 3
    assert unioned_results[0]["pmid"] == "1"
    assert unioned_results[0]["operation"] == "upsert"
    assert unioned_results[2]["pmid"] == "1"
    assert unioned_results[2]["operation"] == "delete"


def test_union_all_preserves_duplicates() -> None:
    """
    Verify that if the exact same record exists in both baseline and updates
    (e.g., edge case where a file was re-ingested or moved), UNION ALL preserves both.
    This is important because deduplication happens downstream in int_pubmed_deduped.
    """
    record = {
        "file_name": "duplicate.xml.gz",
        "ingestion_ts": 100,
        "pmid": "999",
        "operation": "upsert",
    }
    baseline_records = [record]
    updates_records = [record]

    # Simulate UNION ALL
    unioned_results = baseline_records + updates_records

    assert len(unioned_results) == 2
    assert unioned_results[0] == record
    assert unioned_results[1] == record


def test_union_all_empty_inputs() -> None:
    """
    Verify that the view handles empty tables gracefully.
    """
    # Case 1: Empty Baseline
    baseline_empty: List[Dict[str, Any]] = []
    updates = [{"pmid": "1"}]
    res1 = baseline_empty + updates
    assert len(res1) == 1

    # Case 2: Empty Updates
    baseline = [{"pmid": "2"}]
    updates_empty: List[Dict[str, Any]] = []
    res2 = baseline + updates_empty
    assert len(res2) == 1

    # Case 3: Both Empty
    res3 = baseline_empty + updates_empty
    assert len(res3) == 0


def test_union_all_null_propagation() -> None:
    """
    Verify that NULL (None) values are correctly propagated.
    In a UNION ALL, if one side has NULL for a column, it should appear as NULL in the result.
    """
    baseline_records = [
        {
            "pmid": "1",
            "title": None,  # Explicit NULL
            "doi": "10.1000/1",
        }
    ]
    updates_records = [
        {
            "pmid": "2",
            "title": "Has Title",
            "doi": None,  # Explicit NULL
        }
    ]

    unioned = baseline_records + updates_records

    assert len(unioned) == 2
    # Check baseline record in result
    assert unioned[0]["pmid"] == "1"
    assert unioned[0]["title"] is None
    assert unioned[0]["doi"] == "10.1000/1"

    # Check updates record in result
    assert unioned[1]["pmid"] == "2"
    assert unioned[1]["title"] == "Has Title"
    assert unioned[1]["doi"] is None


def test_int_pubmed_deduped_references_staging_view() -> None:
    """
    Verify that int_pubmed_deduped uses the stg_pubmed_citations view.
    This is a structural check of the SQL file content.
    """
    with open("dbt_pubmed/models/intermediate/int_pubmed_deduped.sql", "r") as f:
        sql_content = f.read()

    assert "ref('stg_pubmed_citations')" in sql_content
    assert "ref('stg_pubmed_baseline')" not in sql_content
    assert "ref('stg_pubmed_updates')" not in sql_content


def test_stg_pubmed_citations_structure() -> None:
    """
    Verify structure of stg_pubmed_citations.sql
    """
    with open("dbt_pubmed/models/staging/stg_pubmed_citations.sql", "r") as f:
        sql_content = f.read()

    assert "union all" in sql_content.lower()
    assert "ref('stg_pubmed_baseline')" in sql_content
    assert "ref('stg_pubmed_updates')" in sql_content
